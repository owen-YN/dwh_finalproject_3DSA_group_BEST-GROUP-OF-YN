#!/usr/bin/env python3
import os
import glob
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2 import extras
import io
import csv
import gc 

# CONFIG 
DATA_ROOT = Path(__file__).resolve().parents[1] / "data" / "Project Dataset"

DB_HOST = os.getenv("DB_HOST", "postgres-db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "shopzada_dwh")
DB_USER = os.getenv("DB_USER", "shopzada_admin")
DB_PASS = os.getenv("DB_PASS", "bo_is_dabest")

TABLE_MAPPING = {
    'staging_product_list': ['Business Department/product_list*'],
    'staging_user_data': ['Customer Management Department/user_data*'],
    'staging_user_job': ['Customer Management Department/user_job*'],
    'staging_user_credit_card': ['Customer Management Department/user_credit_card*'],
    'staging_merchant_data': ['Enterprise Department/merchant_data.html'],
    'staging_staff_data': ['Enterprise Department/staff_data.html'],
    'staging_order_with_merchant_data': ['Enterprise Department/order_with_merchant_data*'],
    'staging_campaign_data': ['Marketing Department/campaign_data.csv'],
    'staging_transactional_campaign_data': ['Marketing Department/transactional_campaign_data.csv'],
    'staging_order_data': ['Operations Department/order_data_*'],
    'staging_line_item_prices': ['Operations Department/line_item_data_prices*'],
    'staging_line_item_products': ['Operations Department/line_item_data_products*'],
    'staging_order_delays': ['Operations Department/order_delays.html']
}

# DATA QUALITY CHECK
EXPECTED_SCHEMA = {
    'staging_product_list': ['product_id', 'product_name', 'product_type', 'price'],
    'staging_user_data': ['user_id', 'creation_date', 'name', 'street', 'state', 'city', 'country', 'birthdate', 'gender', 'device_address', 'user_type'],
    'staging_user_job': ['user_id', 'name', 'job_title', 'job_level' ],
    'staging_user_credit_card': ['user_id', 'name', 'credit_card_number', 'issuing_bank'],
    'staging_merchant_data': ['merchant_id', 'creation_date', 'name', 'street', 'state', 'city', 'country', 'contact_number'],
    'staging_staff_data': ['staff_id', 'name', 'job_level', 'street', 'state', 'city', 'country', 'contact_number', 'creation_date'],
    'staging_order_with_merchant_data': ['order_id', 'merchant_id', 'staff_id'],
    'staging_campaign_data': ['campaign_id', 'campaign_name', 'campaign_description', 'discount'],
    'staging_transactional_campaign_data': ['transaction_date', 'campaign_id', 'order_id', 'estimated_arrival', 'availed'],
    'staging_order_data': ['order_id', 'user_id', 'estimated_arrival', 'transaction_date'],
    'staging_line_item_prices': ['order_id', 'price', 'quantity'],
    'staging_line_item_products': ['order_id', 'product_name', 'product_id'],
    'staging_order_delays': ['order_id', 'delay_in_days']
}

# ---------------- UTILS ----------------
def sanitize_column(col):
    col = str(col).strip().lower()
    col = col.replace(" ", "_").replace(":", "_").replace("-", "_")
    col = col.replace(".", "_").replace("/", "_")
    col = "".join(c for c in col if c.isalnum() or c == "_")
    return col if col else "col"

def make_columns_unique(columns):
    seen = {}
    result = []
    for c in columns:
        if c in seen:
            seen[c] += 1
            result.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            result.append(c)
    return result

def map_dtype_to_pg(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    return "TEXT"

# ---------------- CREATE TABLE ----------------
def create_table_from_df(table_name, df, conn, force_text_cols=None):
    if force_text_cols is None:
        force_text_cols = []

    columns = [sanitize_column(c) for c in df.columns]
    cols = []
    for c, orig_c in zip(columns, df.columns):
        if orig_c in force_text_cols:
            cols.append(f'"{c}" TEXT')
        else:
            cols.append(f'"{c}" {map_dtype_to_pg(df[orig_c].dtype)}')

    sql = f'DROP TABLE IF EXISTS "{table_name}" CASCADE; CREATE TABLE "{table_name}" ({", ".join(cols)});'

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def reset_staging_table(table_name, conn):
    print(f"   → Skipping Truncate for {table_name} (Append Mode)")
    pass

# ---------------- SAFE INSERT ----------------
def safe_insert(df, table_name, conn):
    df = df.astype(str).where(pd.notna(df), None)
    columns = [sanitize_column(c) for c in df.columns]

    with conn.cursor() as cur:
        cols = ", ".join(f'"{c}"' for c in columns)
        records = df.to_numpy().tolist()
        extras.execute_values(
            cur,
            f'INSERT INTO "{table_name}" ({cols}) VALUES %s',
            records,
            page_size=500
        )
    conn.commit()

# ---------------- COPY HANDLER ----------------
def copy_to_postgres(df, table_name, conn):
    df_buffer = io.StringIO()
    df.to_csv(df_buffer, index=False, header=False, sep='\t', na_rep='\\N', quoting=csv.QUOTE_NONE, escapechar='\\')
    df_buffer.seek(0)
    with conn.cursor() as cur:
        cur.copy_from(df_buffer, table_name, sep='\t', null='\\N')
    conn.commit()

# ---------------- PICKLE / PARQUET ----------------
def insert_pickle_or_parquet(df, table_name, conn):
    df = df.astype(str).where(pd.notna(df), None)
    create_table_from_df(table_name, df, conn, force_text_cols=df.columns.tolist())
    reset_staging_table(table_name, conn)
    try:
        copy_to_postgres(df, table_name, conn)
        print(f"   ✅ COPY succeeded for {table_name}")
    except Exception as e:
        print(f"      ❌ COPY failed, falling back to SAFE INSERT")
        conn.rollback()
        safe_insert(df, table_name, conn)
        print(f"   ✅ SAFE INSERT succeeded for {table_name}")

# ---------------- FILE LOADER ----------------
def load_csv_with_auto_delimiter(path):
    for sep in [",", "\t"]:
        try:
            df = pd.read_csv(path, sep=sep, dtype=str)
            if df.shape[1] > 1:
                return df
        except:
            continue
    df = pd.read_csv(path, engine="python", dtype=str)
    return df

def load_file(path):
    ext = path.suffix.lower()
    if ext == ".csv":
        return load_csv_with_auto_delimiter(path)
    elif ext in [".xlsx", ".xls"]:
        return pd.read_excel(path, dtype=str)
    elif ext == ".json":
        return pd.read_json(path, dtype=str)
    elif ext in [".pickle", ".pkl"]:
        return pd.read_pickle(path)
    elif ext == ".parquet":
        return pd.read_parquet(path)
    elif ext == ".html":
        df_list = pd.read_html(path)
        return df_list[0]
    else:
        raise ValueError(f"Unsupported file type: {path}")

# ---------------- DATA QUALITY CHECK ----------------
def check_schema_drift(df, table_name, file_name, conn):
    """
    Checks if DataFrame has columns NOT in the EXPECTED_SCHEMA.
    Logs extra columns to audit table and drops them from DataFrame.
    """
    if table_name not in EXPECTED_SCHEMA:
        return df  # No schema defined, skip check

    expected_cols = set(EXPECTED_SCHEMA[table_name])
    current_cols = set([sanitize_column(c) for c in df.columns])
    
    extra_cols = current_cols - expected_cols
    
    if not extra_cols:
        return df # All good

    print(f"      SCHEMA DRIFT DETECTED! Found extra columns: {extra_cols}")
    
    try:
        with conn.cursor() as cur:
            for col in extra_cols:
                cur.execute("""
                    INSERT INTO audit_schema_drift (table_name, file_name, unknown_column_name)
                    VALUES (%s, %s, %s)
                """, (table_name, os.path.basename(file_name), col))
        conn.commit()
        print(f"      Logged drift to audit_schema_drift table")
    except Exception as e:
        print(f"      Failed to log drift: {e}")
        conn.rollback()

    cols_to_drop = []
    for col in df.columns:
        if sanitize_column(col) in extra_cols:
            cols_to_drop.append(col)
            
    df = df.drop(columns=cols_to_drop)
    print(f"      Dropped columns: {cols_to_drop}")
    
    return df

# ---------------- MAIN INGEST ----------------
def ingest_table(table_name, patterns, base_path, conn):
    print(f"\n Processing {table_name}")

    files = []
    for pattern in patterns:
        files.extend(glob.glob(str(base_path / pattern), recursive=True))
    if not files:
        print(f"   No files found for {table_name}")
        return

    frames = []
    use_copy = False
    for f in files:
        print(f"   → Loading {os.path.basename(f)}")
        try:
            df = load_file(Path(f))
            
            # --- DATA QUALITY CHECK ---
            df = check_schema_drift(df, table_name, f, conn)
            # --------------------------

            if f.lower().endswith((".pkl", ".pickle", ".parquet")):
                use_copy = True

            df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
            df.columns = make_columns_unique([sanitize_column(c) for c in df.columns])
            frames.append(df)
        except Exception as e:
            print(f"      Failed reading file: {e}")

    if not frames:
        return

    df = pd.concat(frames, ignore_index=True)

    print(f"   CHECK → Columns loaded: {list(df.columns)}")
    if len(df) > 0:
        print(f"   CHECK → First row: {df.iloc[0].to_dict()}")

    df = df.astype(str).where(pd.notna(df), None)

    if use_copy:
        insert_pickle_or_parquet(df, table_name, conn)
        return

    create_table_from_df(table_name, df, conn, force_text_cols=df.columns.tolist())
    reset_staging_table(table_name, conn)
    try:
        safe_insert(df, table_name, conn)
        print(f"   SAFE INSERT succeeded for {table_name}")
    except Exception as e:
        print(f"      SAFE INSERT failed for {table_name}")
        print(e)
        conn.rollback()

# ---------------- MAIN ----------------
def main():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    print(f" Data root detected → {DATA_ROOT}")

    for table_name, patterns in TABLE_MAPPING.items():
        ingest_table(table_name, patterns, DATA_ROOT, conn)

    conn.close()
    print("\n Ingestion complete!")

if __name__ == "__main__":
    main()