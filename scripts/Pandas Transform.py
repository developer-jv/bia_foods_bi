import os
import pandas as pd
from dotenv import load_dotenv

# For Parquet writes
import pyarrow as pa
import pyarrow.dataset as ds

load_dotenv()

VALIDATED_DIR = os.getenv("VALIDATED_DIR", "./data/validated")
CURATED_DIR = os.getenv("CURATED_DIR", "./data/curated")
os.makedirs(CURATED_DIR, exist_ok=True)

def _read_csv(name: str) -> pd.DataFrame:
    path = os.path.join(VALIDATED_DIR, name)
    df = pd.read_csv(path, dtype=str)  # <-- force string to avoid losing IDs
    df.columns = [c.strip().lower() for c in df.columns]
    # strip/upper for id-like fields commonly used
    for idcol in ["customer_id", "product_id"]:
        if idcol in df.columns:
            df[idcol] = df[idcol].astype(str).str.strip()
    # normalize date if present
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
    # numeric coercions for measures only (never IDs)
    for num in ["quantity", "price", "revenue"]:
        if num in df.columns:
            df[num] = pd.to_numeric(df[num], errors="coerce")
    return df

def _derive_revenue(df: pd.DataFrame) -> pd.DataFrame:
    if "revenue" not in df.columns and {"quantity","price"}.issubset(df.columns):
        df["revenue"] = df["quantity"].astype(float) * df["price"].astype(float)
    return df

def _write_parquet(df: pd.DataFrame, out_dir: str, partition_cols=None):
    os.makedirs(out_dir, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    ds.write_dataset(
        data=table,
        base_dir=out_dir,
        format="parquet",
        partitioning=partition_cols if partition_cols else None,
        existing_data_behavior="overwrite_or_ignore"
    )

def main():
    customers = _read_csv("sap_customers.csv")
    products  = _read_csv("sap_products.csv")
    calendar  = _read_csv("sap_calendar.csv")
    sales     = _derive_revenue(_read_csv("sap_sales.csv"))

    # Enrich sales (left joins on string IDs)
    enriched = sales.copy()

    if "customer_id" in enriched.columns and "customer_id" in customers.columns:
        cust_cols = [c for c in customers.columns if c != "customer_id"]
        enriched = enriched.merge(customers[["customer_id", *cust_cols]].drop_duplicates("customer_id"),
                                  on="customer_id", how="left", suffixes=("", "_cust"))

    if "product_id" in enriched.columns and "product_id" in products.columns:
        prod_cols = [c for c in products.columns if c != "product_id"]
        enriched = enriched.merge(products[["product_id", *prod_cols]].drop_duplicates("product_id"),
                                  on="product_id", how="left", suffixes=("", "_prod"))

    if "date" in enriched.columns and "date" in calendar.columns:
        cal_cols = [c for c in calendar.columns if c != "date"]
        if cal_cols:
            enriched = enriched.merge(calendar[["date", *cal_cols]].drop_duplicates("date"),
                                      on="date", how="left", suffixes=("", "_cal"))

    # Write curated datasets (IDs remain strings)
    _write_parquet(enriched, os.path.join(CURATED_DIR, "sales_enriched"),
                   partition_cols=["date"] if "date" in enriched.columns else None)
    _write_parquet(customers, os.path.join(CURATED_DIR, "dim_customers"))
    _write_parquet(products,  os.path.join(CURATED_DIR, "dim_products"))
    _write_parquet(calendar,  os.path.join(CURATED_DIR, "dim_calendar"))

    print(f"✅ Curated data written to {CURATED_DIR} (IDs preserved as strings)")

if __name__ == "__main__":
    main()
