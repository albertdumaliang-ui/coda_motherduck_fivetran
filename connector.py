import json
import re
import time
import traceback
import requests
from typing import Any, Dict, List, Optional, Tuple

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log

CODA_API_BASE = "https://coda.io/apis/v1"


# -------------------------
# Config helpers
# -------------------------
def get_str(cfg: dict, key: str, default: Optional[str] = None) -> Optional[str]:
    v = cfg.get(key, default)
    if v is None:
        return None
    s = str(v).strip()
    return s if s != "" else None


def get_bool(cfg: dict, key: str, default: bool = False) -> bool:
    v = cfg.get(key)
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in {"true", "1", "yes", "y"}:
        return True
    if s in {"false", "0", "no", "n"}:
        return False
    return default


def get_int(cfg: dict, key: str, default: int) -> int:
    v = cfg.get(key)
    if v is None or str(v).strip() == "":
        return default
    try:
        return int(str(v).strip())
    except ValueError:
        raise ValueError(f"Configuration '{key}' must be an integer string. Got: {v!r}")


def safe_table_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "coda_table"


def safe_col_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "col"


# -------------------------
# Coda API helpers
# -------------------------
def coda_get(token: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{CODA_API_BASE}{path}"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, params=params, timeout=60)
    if not r.ok:
        raise RuntimeError(f"HTTP {r.status_code} GET {url}\n{r.text}")
    return r.json()


def list_columns(token: str, doc_id: str, table_id: str) -> List[Dict[str, Any]]:
    cols: List[Dict[str, Any]] = []
    page_token: Optional[str] = None

    while True:
        params: Dict[str, Any] = {"limit": 100}
        if page_token:
            params["pageToken"] = page_token

        data = coda_get(token, f"/docs/{doc_id}/tables/{table_id}/columns", params=params)
        cols.extend(data.get("items", []) or [])
        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return cols


def parse_query_value(raw: Optional[str]) -> Any:
    """
    Accepts:
      - "true" -> True
      - "123" -> 123
      - "\"abc\"" -> "abc"
      - "abc" -> "abc"
    """
    if raw is None:
        return True
    s = str(raw).strip()
    if s == "":
        return True
    try:
        return json.loads(s)
    except Exception:
        return s


def build_coda_query(column_name: str, value: Any) -> str:
    """
    Coda query syntax: "Column Name":<json_value>
    """
    col = column_name.replace('"', '\\"')
    return f"\"{col}\":{json.dumps(value)}"


def fetch_rows_page(
    token: str,
    doc_id: str,
    table_id: str,
    page_size: int,
    page_token: Optional[str],
    query: Optional[str],
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    FULL SYNC (no syncToken):
      - Uses pageToken only
      - Applies optional query filter (e.g. "to_sync":true)
    """
    params: Dict[str, Any] = {
        "useColumnNames": "true",
        "valueFormat": "simpleWithArrays",
        "limit": page_size,
    }
    if query:
        params["query"] = query
    if page_token:
        params["pageToken"] = page_token

    data = coda_get(token, f"/docs/{doc_id}/tables/{table_id}/rows", params=params)
    items = data.get("items", []) or []
    next_page = data.get("nextPageToken")
    return items, next_page


# -------------------------
# Type mapping: Coda -> Fivetran
# -------------------------
def coda_format_to_fivetran_type(col: Dict[str, Any]) -> str:
    fmt = (col.get("format") or {})
    t = str(fmt.get("type") or "").lower()

    if t in {"number", "slider", "scale", "currency", "percent"}:
        return "DOUBLE"
    if t in {"checkbox", "toggle", "boolean"}:
        return "BOOLEAN"
    if t in {"date", "datetime"}:
        return "UTC_DATETIME"

    return "STRING"


# -------------------------
# Automatic coercion (schema-driven)
# -------------------------
_NULL_STRINGS = {"", "null", "none", "nan", "n/a", "na", "-", "—", "–"}


def _coerce_boolean(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in _NULL_STRINGS:
            return None
        if s in {"true", "t", "1", "yes", "y"}:
            return True
        if s in {"false", "f", "0", "no", "n"}:
            return False
    return None


def _coerce_double(v: Any) -> Optional[float]:
    """
    Converts:
      - 123 -> 123.0
      - "1,234.56" -> 1234.56
      - "69.37%" -> 69.37   (NOTE: change below if you want 0.6937)
      - "" / "—" / "N/A" -> None
    """
    if v is None:
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)

    if isinstance(v, str):
        s = v.strip()
        if s.lower() in _NULL_STRINGS:
            return None

        s = s.replace("%", "").replace(",", "").strip()

        # sometimes currencies may contain symbols
        s = re.sub(r"^[^\d\-\+]*", "", s)          # leading symbols
        s = re.sub(r"[^\d\.\-\+eE]+$", "", s)      # trailing junk

        try:
            num = float(s)
        except ValueError:
            return None

        return num

    return None


def _coerce_by_type(ft_type: str, v: Any) -> Any:
    """
    Apply coercion based on Fivetran column type.
    Only coerce when necessary; otherwise pass through.
    """
    t = (ft_type or "").upper()

    if t == "DOUBLE":
        return _coerce_double(v)
    if t == "BOOLEAN":
        return _coerce_boolean(v)
    # For UTC_DATETIME, Coda already returns ISO strings (createdAt/updatedAt etc.)
    return v


# -------------------------
# Dynamic table configs
# -------------------------
def _normalize_tables_input(raw_tables: Any) -> List[dict]:
    # UI may provide JSON as a string. Accept both.
    if raw_tables is None:
        return []
    if isinstance(raw_tables, str):
        raw_tables = raw_tables.strip()
        if raw_tables == "":
            return []
        try:
            raw_tables = json.loads(raw_tables)
        except Exception as e:
            raise ValueError(f"Configuration 'tables' must be valid JSON. Error: {e}")
    if not isinstance(raw_tables, list):
        raise ValueError("Configuration 'tables' must be a JSON array/list.")
    return raw_tables


def read_table_configs(configuration: dict) -> List[dict]:
    """
    Preferred (dynamic):
      - coda_api_token
      - tables: [{doc_id, table_id, destination_table?, page_size?, query_column?, query_value?, unique_column?}, ...]

    Backward compatible (single):
      - doc_id, table_id, destination_table, page_size, query_column, query_value, unique_column
    """
    token = get_str(configuration, "coda_api_token")
    if not token:
        raise ValueError("Missing required config: coda_api_token")

    raw_tables = _normalize_tables_input(configuration.get("tables"))

    # Backward-compat: if no tables array provided, use legacy single-table config
    if not raw_tables:
        doc_id = get_str(configuration, "doc_id")
        table_id = get_str(configuration, "table_id")
        if not doc_id or not table_id:
            raise ValueError("Missing required config: either 'tables' array OR legacy 'doc_id' + 'table_id'.")

        raw_tables = [{
            "doc_id": doc_id,
            "table_id": table_id,
            "destination_table": get_str(configuration, "destination_table") or table_id,
            "page_size": get_int(configuration, "page_size", 200),
            "query_column": get_str(configuration, "query_column"),
            "query_value": get_str(configuration, "query_value"),
            "unique_column": get_str(configuration, "unique_column"),
        }]

    configs: List[dict] = []
    seen_dest_tables: set = set()

    for idx, t in enumerate(raw_tables, start=1):
        if not isinstance(t, dict):
            raise ValueError(f"tables[{idx}] must be an object/dict.")

        doc_id = get_str(t, "doc_id")
        table_id = get_str(t, "table_id")
        if not doc_id or not table_id:
            raise ValueError(f"Missing doc_id/table_id in tables[{idx}].")

        dest_table = safe_table_name(get_str(t, "destination_table") or table_id)
        if dest_table in seen_dest_tables:
            raise ValueError(
                f"Duplicate destination_table '{dest_table}' in tables[{idx}]. "
                f"Each table entry must write to a unique destination table."
            )
        seen_dest_tables.add(dest_table)

        page_size = int(t.get("page_size", 200))
        if page_size <= 0:
            raise ValueError(f"tables[{idx}].page_size must be > 0")

        query_column = get_str(t, "query_column")
        query_value_raw = get_str(t, "query_value")
        unique_column = get_str(t, "unique_column")

        query_str = None
        if query_column:
            query_val = parse_query_value(query_value_raw)
            query_str = build_coda_query(query_column, query_val)

        configs.append({
            "idx": idx,
            "token": token,
            "doc_id": doc_id,
            "table_id": table_id,
            "dest_table": dest_table,
            "page_size": page_size,
            "query_column": query_column,
            "query_str": query_str,
            "unique_column": unique_column,  # column NAME (useColumnNames=true)
        })

    return configs


# -------------------------
# Fivetran schema + update
# -------------------------
def schema(configuration: dict) -> List[dict]:
    configs = read_table_configs(configuration)

    tables: List[dict] = []
    for cfg in configs:
        token = cfg["token"]
        doc_id = cfg["doc_id"]
        table_id = cfg["table_id"]
        dest_table = cfg["dest_table"]
        unique_column = cfg["unique_column"]

        cols = list_columns(token, doc_id, table_id)

        # Always present system columns from Coda row object
        fivetran_cols: Dict[str, str] = {
            "row_id": "STRING",
            "created_at": "UTC_DATETIME",
            "updated_at": "UTC_DATETIME",
        }

        for c in cols:
            name = c.get("name")
            if not name:
                continue

            col_name = safe_col_name(str(name))
            if col_name in fivetran_cols:
                col_name = f"coda_{col_name}"

            fivetran_cols[col_name] = coda_format_to_fivetran_type(c)

        # PK defaults to row_id unless overridden
        pk = ["row_id"]
        if unique_column:
            pk = [safe_col_name(unique_column)]

        tables.append({
            "table": dest_table,
            "primary_key": pk,
            "columns": fivetran_cols,
        })

    return tables


def _sync_one_table(cfg: dict, state: dict) -> Tuple[int, dict]:
    """
    FULL SYNC (no syncToken). Still upserts using PK.
    Returns: (emitted_count, updated_state)
    """
    token = cfg["token"]
    doc_id = cfg["doc_id"]
    table_id = cfg["table_id"]
    dest_table = cfg["dest_table"]
    page_size = cfg["page_size"]
    query_str = cfg["query_str"]
    unique_column = cfg["unique_column"]
    unique_col_safe = safe_col_name(unique_column) if unique_column else None

    log.info(f"[Table {cfg['idx']}] Start: doc_id={doc_id}, table_id={table_id}, dest_table={dest_table}")
    log.info(f"[Table {cfg['idx']}] page_size={page_size}, query={query_str!r} (FULL SYNC; no syncToken)")

    # Column name -> safe column (keys are column NAMES because useColumnNames=true)
    cols = list_columns(token, doc_id, table_id)

    name_to_safe: Dict[str, str] = {}
    safe_to_type: Dict[str, str] = {}

    for c in cols:
        n = c.get("name")
        if not n:
            continue

        safe = safe_col_name(str(n))
        if safe in {"row_id", "created_at", "updated_at"}:
            safe = f"coda_{safe}"

        name_to_safe[str(n)] = safe
        safe_to_type[safe] = coda_format_to_fivetran_type(c)

    page_token: Optional[str] = None
    emitted = 0

    while True:
        items, page_token = fetch_rows_page(
            token=token,
            doc_id=doc_id,
            table_id=table_id,
            page_size=page_size,
            page_token=page_token,
            query=query_str,
        )

        for row in items:
            values = row.get("values", {}) or {}

            record: Dict[str, Any] = {
                "row_id": row.get("id"),
                "created_at": row.get("createdAt"),
                "updated_at": row.get("updatedAt"),
            }

            for col_name, col_val in values.items():
                safe = name_to_safe.get(col_name, safe_col_name(col_name))
                if safe in record:
                    safe = f"coda_{safe}"

                ft_type = safe_to_type.get(safe, "STRING")
                record[safe] = _coerce_by_type(ft_type, col_val)

            # Ensure PK column exists if user set unique_column
            if unique_column and unique_col_safe:
                if safe_col_name(unique_column) == "row_id":
                    record[unique_col_safe] = row.get("id")
                else:
                    pk_val = values.get(unique_column)
                    if pk_val is None or str(pk_val).strip() == "":
                        pk_val = row.get("id")
                    record[unique_col_safe] = pk_val

            yield op.upsert(table=dest_table, data=record)
            emitted += 1

        if not page_token:
            break

    log.info(f"[Table {cfg['idx']}] Done: emitted={emitted}")
    return emitted, state  # no state changes for full sync


def update(configuration: dict, state: dict):
    configs = read_table_configs(configuration)

    continue_on_error = get_bool(configuration, "continue_on_error", True)

    new_state = dict(state)
    total_emitted = 0
    failures: List[str] = []

    for cfg in configs:
        t0 = time.perf_counter()
        try:
            emitted_before = total_emitted
            emitted_this = 0

            gen = _sync_one_table(cfg, new_state)

            while True:
                try:
                    op_item = next(gen)
                    yield op_item
                except StopIteration as stop:
                    emitted_this, new_state = stop.value  # type: ignore
                    break

            total_emitted = emitted_before + emitted_this
            dt = time.perf_counter() - t0
            log.info(f"[Table {cfg['idx']}] Timing: {dt:.2f}s")

        except Exception as e:
            dt = time.perf_counter() - t0
            msg = f"[Table {cfg['idx']}] FAILED after {dt:.2f}s: {e}"
            failures.append(msg)
            log.severe(msg)
            log.severe(traceback.format_exc())

            if not continue_on_error:
                raise

    # One checkpoint at end for all tables (state unchanged, but required by SDK patterns)
    yield op.checkpoint(state=new_state)

    log.info(f"Run complete. total_emitted={total_emitted}, tables={len(configs)}, failures={len(failures)}")
    if failures:
        for f in failures[:20]:
            log.severe(f)


connector = Connector(update=update, schema=schema)