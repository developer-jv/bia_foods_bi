import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text

load_dotenv()
PG_HOST=os.getenv("PG_HOST","localhost")
PG_PORT=int(os.getenv("PG_PORT","5432"))
PG_DB=os.getenv("PG_DB","bia_dw")
PG_USER=os.getenv("PG_USER","bia_user")
PG_PASSWORD=os.getenv("PG_PASSWORD","bia_password")
PG_SCHEMA=os.getenv("PG_SCHEMA","staging")
CURATED_DIR=os.getenv("CURATED_DIR","./data/curated")

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}", future=True)

def ensure_schema():
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{PG_SCHEMA}"'))

def load_parquet(subdir, table_name):
    path = os.path.join(CURATED_DIR, subdir)
    # Read all Parquet files in folder
    df = pd.read_parquet(path)
    # Convert column names to snake_case, lower
    df.columns = [c.lower() for c in df.columns]
    full_table = f'{PG_SCHEMA}.{table_name}'
    df.to_sql(table_name, engine, schema=PG_SCHEMA, if_exists="replace", index=False, chunksize=5000)
    print(f"✅ Loaded {len(df)} rows into {full_table}")

def main():
    ensure_schema()
    loads = [
        ("sales_enriched", "fct_sales_enriched"),
        ("dim_customers", "dim_customers"),
        ("dim_products", "dim_products"),
        ("dim_calendar", "dim_calendar"),
    ]
    for subdir, table in loads:
        load_parquet(subdir, table)

if __name__ == "__main__":
    main()
