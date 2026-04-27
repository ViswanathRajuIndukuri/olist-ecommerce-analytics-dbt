"""
Download Olist data via kagglehub and load it into DuckDB.

Run once:
    python load_olist.py

Steps:
1. Uses kagglehub to download the Olist dataset (cached automatically).
2. Copies the 9 CSVs into ./data/ (project-local, easy to inspect).
3. Loads each CSV into a `raw` schema in olist.duckdb.

Prerequisites:
    pip install kagglehub duckdb
"""
import shutil
import sys
from pathlib import Path

import duckdb
import kagglehub

# ─── Configuration ───────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = PROJECT_ROOT / "olist.duckdb"
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"

# CSV filename → DuckDB table name
TABLES = {
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_customers_dataset.csv": "customers",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "product_category_name_translation",
}


def download_via_kagglehub() -> Path:
    """Download Olist dataset via kagglehub. Returns the cache path."""
    print(f"Downloading {KAGGLE_DATASET} via kagglehub...")
    cache_path = kagglehub.dataset_download(KAGGLE_DATASET)
    print(f"  Cached at: {cache_path}\n")
    return Path(cache_path)


def copy_to_data_dir(cache_path: Path) -> None:
    """Copy the 9 CSVs from the kagglehub cache into ./data/."""
    DATA_DIR.mkdir(exist_ok=True)

    for csv_name in TABLES:
        src = cache_path / csv_name
        dst = DATA_DIR / csv_name
        if not src.exists():
            sys.exit(f"ERROR: Expected file not in cache: {src}")
        if dst.exists():
            print(f"  Already in data/: {csv_name}")
            continue
        shutil.copy2(src, dst)
        size_mb = dst.stat().st_size / (1024 * 1024)
        print(f"  Copied {csv_name:55s} ({size_mb:.1f} MB)")
    print()


def load_into_duckdb() -> None:
    """Load each CSV into a `raw` schema in DuckDB."""
    print(f"Loading CSVs into {DB_PATH}...\n")

    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    for csv_name, table_name in TABLES.items():
        csv_path = DATA_DIR / csv_name
        print(f"  Loading {csv_name:55s} → raw.{table_name}")
        con.execute(f"DROP TABLE IF EXISTS raw.{table_name}")
        con.execute(
            f"CREATE TABLE raw.{table_name} AS "
            f"SELECT * FROM read_csv_auto('{csv_path}', header=true)"
        )
        count = con.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()[0]
        print(f"    → {count:,} rows")

    print(f"\nWarehouse ready at: {DB_PATH}\n")
    print("Tables in raw schema:")
    for row in con.execute("SHOW TABLES FROM raw").fetchall():
        count = con.execute(f"SELECT COUNT(*) FROM raw.{row[0]}").fetchone()[0]
        print(f"  raw.{row[0]:50s} {count:>10,} rows")
    con.close()


def main() -> None:
    print("=" * 70)
    print("Olist Data Pipeline: Download → Copy → Load")
    print("=" * 70 + "\n")

    cache_path = download_via_kagglehub()
    copy_to_data_dir(cache_path)
    load_into_duckdb()


if __name__ == "__main__":
    main()