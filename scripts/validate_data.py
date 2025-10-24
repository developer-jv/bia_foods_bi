import os
import json
import pandas as pd
from dotenv import load_dotenv
import great_expectations as ge

load_dotenv()

RAW_DIR = os.getenv("RAW_DIR", "./data/raw")
VALIDATED_DIR = os.getenv("VALIDATED_DIR", "./data/validated")
REPORTS_DIR = os.getenv("GE_REPORTS_DIR", "./ge_reports")
os.makedirs(VALIDATED_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

FILES = ["sap_customers.csv","sap_products.csv","sap_calendar.csv","sap_sales.csv"]

def _get_validator(df: pd.DataFrame):
    if hasattr(ge, "from_pandas"):
        return ge.from_pandas(df.copy())
    from great_expectations.dataset import PandasDataset
    return PandasDataset(df.copy())

def _pre(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower() for c in df.columns]
    # IDs must be strings like C00001 / P00001
    for idcol in ["customer_id","product_id"]:
        if idcol in df.columns:
            df[idcol] = df[idcol].astype(str).str.strip().str.upper()
    if "cal_date" in df.columns and "date" not in df.columns:
        df = df.rename(columns={"cal_date":"date"})
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
    # numerics
    for num in ["quantity","price","revenue"]:
        if num in df.columns:
            df[num] = pd.to_numeric(df[num], errors="coerce")
    if "revenue" not in df.columns and {"quantity","price"}.issubset(df.columns):
        df["revenue"] = df["quantity"] * df["price"]
    return df

def validate_file(fname: str):
    path = os.path.join(RAW_DIR, fname)
    df = pd.read_csv(path, dtype=str)
    df = _pre(df)
    v = _get_validator(df)

    # Common
    if "customer_id" in df.columns:
        v.expect_column_values_to_match_regex("customer_id", r"^[Cc][0-9]{5}$", mostly=1.0)
    if "product_id" in df.columns:
        v.expect_column_values_to_match_regex("product_id", r"^[Pp][0-9]{5}$", mostly=1.0)
    if "date" in df.columns:
        v.expect_column_values_to_match_regex("date", r"^\d{4}-\d{2}-\d{2}$", mostly=0.95)
    if "quantity" in df.columns:
        v.expect_column_values_to_be_between("quantity", min_value=0, mostly=1.0)
    if "price" in df.columns:
        v.expect_column_values_to_be_between("price", min_value=0, mostly=1.0)
    if "revenue" in df.columns:
        v.expect_column_values_to_be_between("revenue", min_value=0, mostly=1.0)

    # Required per file
    if "customers" in fname:
        v.expect_column_to_exist("customer_id")
    if "products" in fname:
        v.expect_column_to_exist("product_id")
    if "calendar" in fname:
        v.expect_column_to_exist("date")
    if "sales" in fname:
        for c in ["customer_id","product_id","date"]:
            v.expect_column_to_exist(c)
            v.expect_column_values_to_not_be_null(c)

    res = v.validate()
    report = res if isinstance(res, dict) else {
        "success": bool(getattr(res, "success", False))
    }
    out_json = os.path.join(REPORTS_DIR, fname.replace(".csv","_ge_report.json"))
    with open(out_json,"w",encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    if (report.get("success", False) if isinstance(report, dict) else report["success"]):
        df.to_csv(os.path.join(VALIDATED_DIR, fname), index=False)
        print(f"✅ {fname} VALID")
    else:
        print(f"❌ {fname} INVALID -> {out_json}")
        raise SystemExit(1)

def main():
    ok = True
    for f in FILES:
        try:
            validate_file(f)
        except SystemExit:
            ok = False
        except Exception as e:
            print("Error:", e)
            ok = False
    if not ok:
        raise SystemExit(1)

if __name__ == "__main__":
    main()
