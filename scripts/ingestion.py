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
    'staging_product_list': ['Business Department/product_list.xlsx'],
    'staging_user_data': ['Customer Management Department/user_data.json'],
    'staging_user_job': ['Customer Management Department/user_job.csv'],
    'staging_user_credit_card': ['Customer Management Department/user_credit_card.pickle'],
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

# ---------------- UTILS ----------------
def sanitize_column(col):
    col = col.strip().lower()
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

    # sanitize column names
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
    # CHANGE: Commented out TRUNCATE logic for Incremental/Append Simulation
    # with conn.cursor() as cur:
    #     print(f"   → Resetting table {table_name} (TRUNCATE RESTART IDENTITY)")
    #     cur.execute(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE;')
    # conn.commit()
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
    # write as TSV to avoid issues with commas in strings
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
    # fallback
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

# ---------------- MAIN INGEST ----------------
def ingest_table(table_name, patterns, base_path, conn):
    print(f"\n📦 Processing {table_name}")

    # 1️⃣ Find files
    files = []
    for pattern in patterns:
        files.extend(glob.glob(str(base_path / pattern), recursive=True))
    if not files:
        print(f"   ⚠ No files found for {table_name}")
        return

    # 2️⃣ Load files
    frames = []
    use_copy = False
    for f in files:
        print(f"   → Loading {os.path.basename(f)}")
        try:
            df = load_file(Path(f))
            if f.lower().endswith((".pkl", ".pickle", ".parquet")):
                use_copy = True

            df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
            df.columns = make_columns_unique([sanitize_column(c) for c in df.columns])
            frames.append(df)
        except Exception as e:
            print(f"      ❌ Failed reading file: {e}")

    if not frames:
        return

    df = pd.concat(frames, ignore_index=True)

    print(f"   CHECK → Columns loaded: {list(df.columns)}")
    if len(df) > 0:
        print(f"   CHECK → First row: {df.iloc[0].to_dict()}")

    df = df.astype(str).where(pd.notna(df), None)

    # 3️⃣ For Pickle / Parquet → COPY
    if use_copy:
        insert_pickle_or_parquet(df, table_name, conn)
        return

    # 4️⃣ For CSV / Excel / JSON / HTML → SAFE INSERT
    create_table_from_df(table_name, df, conn, force_text_cols=df.columns.tolist())
    reset_staging_table(table_name, conn)
    try:
        safe_insert(df, table_name, conn)
        print(f"   ✅ SAFE INSERT succeeded for {table_name}")
    except Exception as e:
        print(f"      ❌ SAFE INSERT failed for {table_name}")
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

    print(f"📁 Data root detected → {DATA_ROOT}")

    for table_name, patterns in TABLE_MAPPING.items():
        ingest_table(table_name, patterns, DATA_ROOT, conn)

    conn.close()
    print("\n🎉 Ingestion complete!")

if __name__ == "__main__":
    main()