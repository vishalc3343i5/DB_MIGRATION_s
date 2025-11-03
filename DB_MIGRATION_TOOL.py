import streamlit as st
import pandas as pd
import json
from sqlalchemy import create_engine, text
from pymongo import MongoClient, ReplaceOne
from bson import Int64, Decimal128, ObjectId
from datetime import datetime
import bson



# ---------------------- PAGE SETUP ----------------------
st.set_page_config(page_title="🧠 Smart SQL → MongoDB Migrator", layout="wide")
st.title("🧩 Smart SQL → MongoDB Migration Tool")

st.markdown("""
This tool helps you **migrate data** from **MySQL** or **MS SQL Server** to **MongoDB**  
with support for data type mapping, schema preview, and relationship handling.
""")

st.divider()

# ---------------------- STEP 1: CONNECTION CONFIG ----------------------
st.header("1️⃣ Connect Databases")

db_type = st.radio("Select SQL Engine:", ["MySQL", "MS SQL Server"], horizontal=True)

with st.form("connection_form"):
    col1, col2 = st.columns(2)
    with col1:
        sql_host = st.text_input("SQL Host", "localhost")
        sql_db = st.text_input("SQL Database", "SampleDB")
        sql_user = st.text_input("SQL Username", "sa" if db_type == "MS SQL Server" else "root")
        sql_pass = st.text_input("SQL Password", type="password")
        sql_port = st.text_input("SQL Port", "1433" if db_type == "MS SQL Server" else "3306")
        sql_pass = sql_pass.replace('@', '%40')

    with col2:
        if db_type == "MS SQL Server":
            sql_driver = st.text_input("ODBC Driver", "ODBC Driver 17 for SQL Server")
        else:
            sql_driver = None
        mongo_uri = st.text_input("MongoDB URI", "mongodb://localhost:27017/")
        mongo_db = st.text_input("Target MongoDB Database", "MigratedDB")

    submitted = st.form_submit_button("Connect Databases 🚀")

if submitted:
    try:
        if db_type == "MySQL":
            conn_str = f"mysql+pymysql://{sql_user}:{sql_pass}@{sql_host}:{sql_port}/{sql_db}"
        else:
            conn_str = f"mssql+pyodbc://{sql_user}:{sql_pass}@{sql_host}/{sql_db}?driver={sql_driver.replace(' ', '+')}"
        print(conn_str)
        engine = create_engine(conn_str)
        mongo_client = MongoClient(mongo_uri)
        mongo_db_obj = mongo_client[mongo_db]
        st.session_state["engine"] = engine
        st.session_state["mongo_db_obj"] = mongo_db_obj
        st.session_state["db_type"] = db_type
        st.success(f"✅ Connected to {db_type} and MongoDB successfully!")
    except Exception as e:
        st.error(f"❌ Connection failed: {e}")
        st.stop()
else:
    if "engine" not in st.session_state:
        st.stop()

engine = st.session_state["engine"]
mongo_db_obj = st.session_state["mongo_db_obj"]
db_type = st.session_state["db_type"]

st.divider()

# ---------------------- STEP 2: TABLES & RELATIONSHIPS ----------------------
st.header("2️⃣ Select Tables & Relationships")

def get_tables(engine, db_type):
    if db_type == "MySQL":
        q = text("SHOW TABLES")
        df = pd.read_sql(q, engine)
        return df.iloc[:, 0].tolist()
    else:
        q = text("SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS FullTable FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
        df = pd.read_sql(q, engine)
        return df["FullTable"].tolist()

def get_foreign_keys(engine, db_type, table):
    if db_type == "MySQL":
        q = text("""
            SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE REFERENCED_TABLE_NAME IS NOT NULL
              AND TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = :table
        """)
        return pd.read_sql(q, engine, params={"table": table})
    else:
        q = text("""
            SELECT 
                tp.name AS ParentTable,
                cp.name AS ParentColumn,
                tr.name AS ReferencedTable,
                cr.name AS ReferencedColumn
            FROM sys.foreign_keys fk
            INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            INNER JOIN sys.tables tp ON fkc.parent_object_id = tp.object_id
            INNER JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
            INNER JOIN sys.tables tr ON fkc.referenced_object_id = tr.object_id
            INNER JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
        """)
        df = pd.read_sql(q, engine)
        return df[df["ParentTable"] == table.split(".")[-1]]

tables = get_tables(engine, db_type)
selected_table = st.selectbox("Select main table to migrate", tables)

fks = get_foreign_keys(engine, db_type, selected_table)
if fks.empty:
    st.info("No relationships detected for this table.")
else:
    st.markdown("### Detected Relationships:")
    rels = []
    for _, r in fks.iterrows():
        rels.append({
            "child": r.get("ParentTable", r.get("TABLE_NAME")),
            "child_col": r.get("ParentColumn", r.get("COLUMN_NAME")),
            "parent": r.get("ReferencedTable", r.get("REFERENCED_TABLE_NAME")),
            "parent_col": r.get("ReferencedColumn", r.get("REFERENCED_COLUMN_NAME")),
            "strategy": "Embed"
        })
    rel_df = pd.DataFrame(rels)
    for idx, rel in rel_df.iterrows():
        rel_df.loc[idx, "strategy"] = st.selectbox(
            f"{rel['child']} → {rel['parent']}",
            ["Embed", "Reference", "Ignore"],
            key=f"rel_{idx}"
        )
    st.dataframe(rel_df)

st.divider()

# ---------------------- STEP 3: FIELD MAPPING & DATA TYPES ----------------------
st.header("3️⃣ Field Mapping & Data Type Conversion")

# Sample data
if db_type == "MySQL":
    sample_df = pd.read_sql(text(f"SELECT * FROM {selected_table} LIMIT 5"), engine)
else:
    sample_df = pd.read_sql(text(f"SELECT TOP 5 * FROM {selected_table}"), engine)

st.dataframe(sample_df)

# Column metadata
if db_type == "MySQL":
    meta_q = text("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table
    """)
else:
    meta_q = text("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = :table
    """)

meta_df = pd.read_sql(meta_q, engine, params={"table": selected_table.split(".")[-1]})

default_type_map = {
    "int": "int32", "bigint": "int64", "smallint": "int32", "tinyint": "bool",
    "varchar": "string", "nvarchar": "string", "text": "string",
    "datetime": "datetime", "timestamp": "datetime", "date": "date",
    "decimal": "decimal", "float": "float", "real": "float",
    "json": "dict", "bit": "bool", "char": "string"
}

mongo_type_options = [
    "string", "int32", "int64", "float", "decimal",
    "bool", "date", "datetime", "array", "dict", "objectid", "null"
]

rename_map, type_map = {}, {}
st.subheader("🗂️ Column Mapping")

for _, row in meta_df.iterrows():
    col = row["COLUMN_NAME"]
    sql_type = str(row["DATA_TYPE"]).lower()
    c1, c2 = st.columns([2, 2])
    with c1:
        rename_map[col] = st.text_input(f"Rename `{col}`", value=col, key=f"rename_{col}")
    with c2:
        default_type = default_type_map.get(sql_type, "string")
        if default_type not in mongo_type_options:
            default_type = "string"
        type_map[col] = st.selectbox(
            f"Type `{col}` ({sql_type}) → Mongo Type",
            mongo_type_options,
            index=mongo_type_options.index(default_type),
            key=f"type_{col}"
        )

st.info("💡 Adjust types to control how each field is stored in MongoDB.")

st.divider()

# ---------------------- STEP 4: SCHEMA PREVIEW ----------------------
st.header("4️⃣ Schema Preview")

def convert_value(v, target_type):
    """Convert SQL value to a MongoDB-compatible Python/BSON type."""
    if pd.isna(v) or v is None:
        return None
    try:
        if target_type in ["int", "int32", "int64"]:
            return int(v)
        if target_type in ["float", "decimal"]:
            # Use Decimal128 for exact precision, otherwise float
            try:
                return Decimal128(str(v))
            except:
                return float(v)
        if target_type == "bool":
            if isinstance(v, (int, float)):
                return bool(v)
            return str(v).lower() in ["true", "1", "yes"]
        if target_type in ["date", "datetime"]:
            if isinstance(v, datetime):
                return v
            try:
                return pd.to_datetime(v).to_pydatetime()
            except:
                return None
        if target_type == "array":
            if isinstance(v, list):
                return v
            try:
                arr = json.loads(v)
                return arr if isinstance(arr, list) else [v]
            except:
                return [v]
        if target_type == "dict":
            if isinstance(v, dict):
                return v
            try:
                obj = json.loads(v)
                return obj if isinstance(obj, dict) else {"value": v}
            except:
                return {"value": v}
        if target_type == "objectid":
            try:
                return ObjectId(str(v))
            except:
                return ObjectId()
        if target_type in ["string", "text"]:
            return str(v)
        return None
    except Exception:
        return None

preview_docs = []
for _, row in sample_df.iterrows():
    doc = {rename_map[k]: convert_value(v, type_map.get(k, "string")) for k, v in row.items()}
    preview_docs.append(doc)

st.json(preview_docs[:3])
st.success("✅ Schema looks ready for migration!")

st.divider()

# ---------------------- STEP 5: MIGRATION ----------------------
st.header("5️⃣ Run Migration")

batch_size = st.number_input("Batch size", min_value=100, max_value=10000, value=500)
upsert = st.checkbox("Use upsert (replace if exists)", value=True)
id_field = st.selectbox("Use which field as _id?", ["(auto)"] + list(sample_df.columns))
start = st.button("🚀 Start Migration")

if start:
    st.info("🔄 Migration in progress... Please wait.")
    coll = mongo_db_obj[selected_table.replace('.', '_')]
    total = pd.read_sql(text(f"SELECT COUNT(*) AS cnt FROM {selected_table}"), engine)["cnt"][0]
    offset, processed = 0, 0
    progress = st.progress(0)
    log = st.empty()

    while True:
        if db_type == "MySQL":
            q = text(f"SELECT * FROM {selected_table} LIMIT :bs OFFSET :o")
        else:
            q = text(f"SELECT * FROM {selected_table} ORDER BY (SELECT NULL) OFFSET :o ROWS FETCH NEXT :bs ROWS ONLY")
        df = pd.read_sql(q, engine, params={"bs": batch_size, "o": offset})
        if df.empty:
            break

        docs = []
        for _, row in df.iterrows():
            doc = {rename_map[k]: convert_value(v, type_map.get(k, "string")) for k, v in row.items()}
            if id_field != "(auto)" and id_field in doc:
                doc["_id"] = doc[id_field]
            docs.append(doc)

        # Validate BSON
        valid_docs = []
        for d in docs:
            try:
                bson.encode(d)
                valid_docs.append(d)
            except Exception:
                pass

        if valid_docs:
            if upsert and "_id" in valid_docs[0]:
                ops = [ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in valid_docs]
                coll.bulk_write(ops, ordered=False)
            else:
                coll.insert_many(valid_docs)

        offset += batch_size
        processed += len(valid_docs)
        progress.progress(min(processed / total, 1.0))
        log.write(f"Processed {processed}/{total} rows...")

    st.success("✅ Migration completed successfully!")
    st.markdown("🎯 Your MongoDB collection is now ready!")
