# Coda → MotherDuck Custom Connector (Fivetran SDK)

This connector syncs **Coda tables into MotherDuck** using the **Fivetran Connector SDK**.

It is designed for teams using **Coda as an operational system** while storing structured data in **MotherDuck for analytics, application logic, or downstream systems**.

The connector extracts rows from Coda based on defined rules and **upserts them into MotherDuck automatically**.

---

# Architecture

```
Coda (Operational Tables)
        │
        ▼
Fivetran Custom Connector
        │
        ▼
MotherDuck (Warehouse / Application Database)
```

This allows:

- **Coda** → editing interface  
- **Fivetran** → ingestion layer  
- **MotherDuck** → queryable data store  

---

# Why Use This Instead of Direct MCP Writes?

Direct MCP writes from Coda or Zapier are useful for **single-event SQL actions**.

This connector is better when you need:

- Continuous table syncing
- Incremental extraction
- Controlled ingestion rules
- Multi-table support
- Reliable schema handling

| Approach | Best For |
|--------|--------|
| Direct MCP Write | Event-based inserts or updates |
| Zapier MCP Write | Automation triggers |
| **This Connector** | Structured table ingestion |

---

# Requirements

You must have:

- Python **3.8+**
- **Fivetran Connector SDK**
- A **Coda API Token**
- A **MotherDuck destination configured in Fivetran**

---

# Step 1 — Clone the Repository

```bash
git clone https://github.com/albertdumaliang-ui/motherduck-to-coda.git
cd motherduck-to-coda
```

---

# Step 2 — Create Virtual Environment

### Mac / Linux

```bash
python3 -m venv env
source env/bin/activate
```

### Windows

```bash
python -m venv env
env\Scripts\activate
```

---

# Step 3 — Install Dependencies

```bash
pip install -r requirements.txt
```

---

# Step 4 — Verify Fivetran CLI

Run:

```bash
fivetran
```

If installed correctly you should see the CLI command list.

---

# Step 5 — Configure the Connector

Edit the configuration file:

```
configuration.json
```

Example structure:

```json
{
  "api_token": "CODA_API_TOKEN",
  "tables": "[{\"doc_id\":\"DOC_ID\",\"table_id\":\"TABLE_ID\",\"destination_table\":\"target_table\",\"page_size\":100,\"query_column\":\"to_sync\",\"query_value\":true,\"unique_column\":\"row_id\"}]"
}
```

---

# Step 6 — Prepare Your Coda Table

Each Coda table **must contain these columns**.

| Column | Type | Purpose |
|------|------|------|
| row_id | Text | Primary key |
| updated_at | DateTime | Last update timestamp |
| to_sync | Boolean | Controls extraction |

Example schema:

```
row_id
updated_at
to_sync
name
email
status
```

---

# Step 7 — Configure the Primary Key

### Case 1 — One Table → One Warehouse Table

Use Coda's internal row ID:

```
RowId()
```

### Case 2 — Multiple Tables → One Warehouse Table

Define your own unique key.

Example:

```
meeting_id
```

---

# Step 8 — Configure Incremental Logic

The connector extracts rows where:

```
to_sync = true
```

Recommended Coda formula:

```
to_sync = updated_at >= Now() - 1.25 hours
```

Why this works:

- 1 hour Fivetran sync interval  
- 15 minute safety buffer  
- prevents missed updates during API calls  

---

# Step 9 — Get Required IDs

## Get Coda Doc ID

In Coda:

```
Top right menu → Copy Doc ID
```

Enable **Developer Mode** if you cannot see it.

---

## Get Table ID

Right-click the table:

```
Copy Table ID
```

---

# Step 10 — Configure Tables

`tables` is a **list of table definitions**.

Example:

```json
"tables": "[{
  \"doc_id\": \"doc123\",
  \"table_id\": \"grid-abc\",
  \"destination_table\": \"transcripts\",
  \"page_size\": 10,
  \"query_column\": \"to_sync\",
  \"query_value\": true,
  \"unique_column\": \"row_id\"
}]"
```

Notes:

- `tables` must be **stringified JSON**
- Use a JSON stringify tool or VSCode extension

---

# Step 11 — Test the Connector

Before deploying run:

```bash
fivetran reset
fivetran debug --configuration configuration.json
```

If successful you should see:

```
Sync succeeded
```

If not, the terminal will display the full error.

---

# Step 12 — Deploy the Connector

Run:

```bash
fivetran deploy \
--api-key 'BASE64_ENCODED_KEY' \
--destination DESTINATION_NAME \
--connection CONNECTION_NAME \
--configuration configuration.json
```

---

# Step 13 — Get Fivetran API Key

In Fivetran:

```
Profile → API Key
Generate Secret
```

Copy the **Base64 Encoded Key**.

---

# Step 14 — Run Initial Sync

In the **Fivetran UI**:

```
Connections → Your Connector → Start Initial Sync
```

Rows synced depend on:

```
to_sync = true
```

---

# Step 15 — Verify in MotherDuck

Destination structure:

```
DESTINATION_NAME
  └ Schema = connection name
        └ destination_table
```

You should see:

- Automatically created schema
- All Coda columns
- Synced records

---

# Managing Columns

If some Coda columns are not needed:

In Fivetran:

```
Schema → Uncheck columns → Save
```

You can optionally drop them from the destination.

---

# Best Practices

### Use Boolean Controlled Sync

```
to_sync
```

Prevents full reloads.

---

### Add Time Buffer

Avoid race conditions during extraction.

```
Now() - 1.25 hours
```

---

### Use Smaller Page Size for Large Text

For transcript-heavy tables:

```
page_size = 10
```

---

# Intended Use

This connector works best when:

- Coda is used as an operational UI
- MotherDuck is used as a warehouse
- tables require incremental syncing
- transcripts or large payloads are stored
- multi-table ingestion is needed

---

# Repository Structure

```
connector.py
configuration.json
requirements.txt
README.md
```

---

# License

MIT
