#!/usr/bin/env python3
"""
Convert TPC-DS .dat files to Parquet and upload to MinIO (S3).

Uses DuckDB for fast CSV-to-Parquet conversion with proper schemas.
Uses boto3 (or mc CLI fallback) for MinIO upload.

Usage:
    python convert_tpcds_to_parquet.py [--scale 20] [--upload] [--bucket tpcds]
    python convert_tpcds_to_parquet.py --scale 20 --upload --endpoint http://localhost:9000
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb required. Install with: pip install duckdb")
    sys.exit(1)

SCRIPT_DIR = Path(__file__).parent

# TPC-DS table schemas: (table_name, [(col_name, col_type), ...])
# Derived from 03-tpcds-tables.sql — column order matches dsdgen .dat output
TPCDS_SCHEMAS = {
    "call_center": [
        ("cc_call_center_sk", "INTEGER"), ("cc_call_center_id", "VARCHAR"), ("cc_rec_start_date", "DATE"),
        ("cc_rec_end_date", "DATE"), ("cc_closed_date_sk", "INTEGER"), ("cc_open_date_sk", "INTEGER"),
        ("cc_name", "VARCHAR"), ("cc_class", "VARCHAR"), ("cc_employees", "INTEGER"), ("cc_sq_ft", "INTEGER"),
        ("cc_hours", "VARCHAR"), ("cc_manager", "VARCHAR"), ("cc_mkt_id", "INTEGER"), ("cc_mkt_class", "VARCHAR"),
        ("cc_mkt_desc", "VARCHAR"), ("cc_market_manager", "VARCHAR"), ("cc_division", "INTEGER"),
        ("cc_division_name", "VARCHAR"), ("cc_company", "INTEGER"), ("cc_company_name", "VARCHAR"),
        ("cc_street_number", "VARCHAR"), ("cc_street_name", "VARCHAR"), ("cc_street_type", "VARCHAR"),
        ("cc_suite_number", "VARCHAR"), ("cc_city", "VARCHAR"), ("cc_county", "VARCHAR"), ("cc_state", "VARCHAR"),
        ("cc_zip", "VARCHAR"), ("cc_country", "VARCHAR"), ("cc_gmt_offset", "DECIMAL(5,2)"),
        ("cc_tax_percentage", "DECIMAL(5,2)"),
    ],
    "catalog_page": [
        ("cp_catalog_page_sk", "INTEGER"), ("cp_catalog_page_id", "VARCHAR"), ("cp_start_date_sk", "INTEGER"),
        ("cp_end_date_sk", "INTEGER"), ("cp_department", "VARCHAR"), ("cp_catalog_number", "INTEGER"),
        ("cp_catalog_page_number", "INTEGER"), ("cp_description", "VARCHAR"), ("cp_type", "VARCHAR"),
    ],
    "catalog_returns": [
        ("cr_returned_date_sk", "INTEGER"), ("cr_returned_time_sk", "INTEGER"), ("cr_item_sk", "INTEGER"),
        ("cr_refunded_customer_sk", "INTEGER"), ("cr_refunded_cdemo_sk", "INTEGER"),
        ("cr_refunded_hdemo_sk", "INTEGER"), ("cr_refunded_addr_sk", "INTEGER"),
        ("cr_returning_customer_sk", "INTEGER"), ("cr_returning_cdemo_sk", "INTEGER"),
        ("cr_returning_hdemo_sk", "INTEGER"), ("cr_returning_addr_sk", "INTEGER"),
        ("cr_call_center_sk", "INTEGER"), ("cr_catalog_page_sk", "INTEGER"), ("cr_ship_mode_sk", "INTEGER"),
        ("cr_warehouse_sk", "INTEGER"), ("cr_reason_sk", "INTEGER"), ("cr_order_number", "BIGINT"),
        ("cr_return_quantity", "INTEGER"), ("cr_return_amount", "DECIMAL(7,2)"),
        ("cr_return_tax", "DECIMAL(7,2)"), ("cr_return_amt_inc_tax", "DECIMAL(7,2)"),
        ("cr_fee", "DECIMAL(7,2)"), ("cr_return_ship_cost", "DECIMAL(7,2)"),
        ("cr_refunded_cash", "DECIMAL(7,2)"), ("cr_reversed_charge", "DECIMAL(7,2)"),
        ("cr_store_credit", "DECIMAL(7,2)"), ("cr_net_loss", "DECIMAL(7,2)"),
    ],
    "catalog_sales": [
        ("cs_sold_date_sk", "INTEGER"), ("cs_sold_time_sk", "INTEGER"), ("cs_ship_date_sk", "INTEGER"),
        ("cs_bill_customer_sk", "INTEGER"), ("cs_bill_cdemo_sk", "INTEGER"), ("cs_bill_hdemo_sk", "INTEGER"),
        ("cs_bill_addr_sk", "INTEGER"), ("cs_ship_customer_sk", "INTEGER"), ("cs_ship_cdemo_sk", "INTEGER"),
        ("cs_ship_hdemo_sk", "INTEGER"), ("cs_ship_addr_sk", "INTEGER"), ("cs_call_center_sk", "INTEGER"),
        ("cs_catalog_page_sk", "INTEGER"), ("cs_ship_mode_sk", "INTEGER"), ("cs_warehouse_sk", "INTEGER"),
        ("cs_item_sk", "INTEGER"), ("cs_promo_sk", "INTEGER"), ("cs_order_number", "BIGINT"),
        ("cs_quantity", "INTEGER"), ("cs_wholesale_cost", "DECIMAL(7,2)"), ("cs_list_price", "DECIMAL(7,2)"),
        ("cs_sales_price", "DECIMAL(7,2)"), ("cs_ext_discount_amt", "DECIMAL(7,2)"),
        ("cs_ext_sales_price", "DECIMAL(7,2)"), ("cs_ext_wholesale_cost", "DECIMAL(7,2)"),
        ("cs_ext_list_price", "DECIMAL(7,2)"), ("cs_ext_tax", "DECIMAL(7,2)"),
        ("cs_coupon_amt", "DECIMAL(7,2)"), ("cs_ext_ship_cost", "DECIMAL(7,2)"),
        ("cs_net_paid", "DECIMAL(7,2)"), ("cs_net_paid_inc_tax", "DECIMAL(7,2)"),
        ("cs_net_paid_inc_ship", "DECIMAL(7,2)"), ("cs_net_paid_inc_ship_tax", "DECIMAL(7,2)"),
        ("cs_net_profit", "DECIMAL(7,2)"),
    ],
    "customer": [
        ("c_customer_sk", "INTEGER"), ("c_customer_id", "VARCHAR"), ("c_current_cdemo_sk", "INTEGER"),
        ("c_current_hdemo_sk", "INTEGER"), ("c_current_addr_sk", "INTEGER"),
        ("c_first_shipto_date_sk", "INTEGER"), ("c_first_sales_date_sk", "INTEGER"),
        ("c_salutation", "VARCHAR"), ("c_first_name", "VARCHAR"), ("c_last_name", "VARCHAR"),
        ("c_preferred_cust_flag", "VARCHAR"), ("c_birth_day", "INTEGER"), ("c_birth_month", "INTEGER"),
        ("c_birth_year", "INTEGER"), ("c_birth_country", "VARCHAR"), ("c_login", "VARCHAR"),
        ("c_email_address", "VARCHAR"), ("c_last_review_date_sk", "INTEGER"),
    ],
    "customer_address": [
        ("ca_address_sk", "INTEGER"), ("ca_address_id", "VARCHAR"), ("ca_street_number", "VARCHAR"),
        ("ca_street_name", "VARCHAR"), ("ca_street_type", "VARCHAR"), ("ca_suite_number", "VARCHAR"),
        ("ca_city", "VARCHAR"), ("ca_county", "VARCHAR"), ("ca_state", "VARCHAR"), ("ca_zip", "VARCHAR"),
        ("ca_country", "VARCHAR"), ("ca_gmt_offset", "DECIMAL(5,2)"), ("ca_location_type", "VARCHAR"),
    ],
    "customer_demographics": [
        ("cd_demo_sk", "INTEGER"), ("cd_gender", "VARCHAR"), ("cd_marital_status", "VARCHAR"),
        ("cd_education_status", "VARCHAR"), ("cd_purchase_estimate", "INTEGER"),
        ("cd_credit_rating", "VARCHAR"), ("cd_dep_count", "INTEGER"),
        ("cd_dep_employed_count", "INTEGER"), ("cd_dep_college_count", "INTEGER"),
    ],
    "date_dim": [
        ("d_date_sk", "INTEGER"), ("d_date_id", "VARCHAR"), ("d_date", "DATE"),
        ("d_month_seq", "INTEGER"), ("d_week_seq", "INTEGER"), ("d_quarter_seq", "INTEGER"),
        ("d_year", "INTEGER"), ("d_dow", "INTEGER"), ("d_moy", "INTEGER"), ("d_dom", "INTEGER"),
        ("d_qoy", "INTEGER"), ("d_fy_year", "INTEGER"), ("d_fy_quarter_seq", "INTEGER"),
        ("d_fy_week_seq", "INTEGER"), ("d_day_name", "VARCHAR"), ("d_quarter_name", "VARCHAR"),
        ("d_holiday", "VARCHAR"), ("d_weekend", "VARCHAR"), ("d_following_holiday", "VARCHAR"),
        ("d_first_dom", "INTEGER"), ("d_last_dom", "INTEGER"), ("d_same_day_ly", "INTEGER"),
        ("d_same_day_lq", "INTEGER"), ("d_current_day", "VARCHAR"), ("d_current_week", "VARCHAR"),
        ("d_current_month", "VARCHAR"), ("d_current_quarter", "VARCHAR"), ("d_current_year", "VARCHAR"),
    ],
    "household_demographics": [
        ("hd_demo_sk", "INTEGER"), ("hd_income_band_sk", "INTEGER"), ("hd_buy_potential", "VARCHAR"),
        ("hd_dep_count", "INTEGER"), ("hd_vehicle_count", "INTEGER"),
    ],
    "income_band": [
        ("ib_income_band_sk", "INTEGER"), ("ib_lower_bound", "INTEGER"), ("ib_upper_bound", "INTEGER"),
    ],
    "inventory": [
        ("inv_date_sk", "INTEGER"), ("inv_item_sk", "INTEGER"), ("inv_warehouse_sk", "INTEGER"),
        ("inv_quantity_on_hand", "INTEGER"),
    ],
    "item": [
        ("i_item_sk", "INTEGER"), ("i_item_id", "VARCHAR"), ("i_rec_start_date", "DATE"),
        ("i_rec_end_date", "DATE"), ("i_item_desc", "VARCHAR"), ("i_current_price", "DECIMAL(7,2)"),
        ("i_wholesale_cost", "DECIMAL(7,2)"), ("i_brand_id", "INTEGER"), ("i_brand", "VARCHAR"),
        ("i_class_id", "INTEGER"), ("i_class", "VARCHAR"), ("i_category_id", "INTEGER"),
        ("i_category", "VARCHAR"), ("i_manufact_id", "INTEGER"), ("i_manufact", "VARCHAR"),
        ("i_size", "VARCHAR"), ("i_formulation", "VARCHAR"), ("i_color", "VARCHAR"),
        ("i_units", "VARCHAR"), ("i_container", "VARCHAR"), ("i_manager_id", "INTEGER"),
        ("i_product_name", "VARCHAR"),
    ],
    "promotion": [
        ("p_promo_sk", "INTEGER"), ("p_promo_id", "VARCHAR"), ("p_start_date_sk", "INTEGER"),
        ("p_end_date_sk", "INTEGER"), ("p_item_sk", "INTEGER"), ("p_cost", "DECIMAL(15,2)"),
        ("p_response_target", "INTEGER"), ("p_promo_name", "VARCHAR"), ("p_channel_dmail", "VARCHAR"),
        ("p_channel_email", "VARCHAR"), ("p_channel_catalog", "VARCHAR"), ("p_channel_tv", "VARCHAR"),
        ("p_channel_radio", "VARCHAR"), ("p_channel_press", "VARCHAR"), ("p_channel_event", "VARCHAR"),
        ("p_channel_demo", "VARCHAR"), ("p_channel_details", "VARCHAR"), ("p_purpose", "VARCHAR"),
        ("p_discount_active", "VARCHAR"),
    ],
    "reason": [
        ("r_reason_sk", "INTEGER"), ("r_reason_id", "VARCHAR"), ("r_reason_desc", "VARCHAR"),
    ],
    "ship_mode": [
        ("sm_ship_mode_sk", "INTEGER"), ("sm_ship_mode_id", "VARCHAR"), ("sm_type", "VARCHAR"),
        ("sm_code", "VARCHAR"), ("sm_carrier", "VARCHAR"), ("sm_contract", "VARCHAR"),
    ],
    "store": [
        ("s_store_sk", "INTEGER"), ("s_store_id", "VARCHAR"), ("s_rec_start_date", "DATE"),
        ("s_rec_end_date", "DATE"), ("s_closed_date_sk", "INTEGER"), ("s_store_name", "VARCHAR"),
        ("s_number_employees", "INTEGER"), ("s_floor_space", "INTEGER"), ("s_hours", "VARCHAR"),
        ("s_manager", "VARCHAR"), ("s_market_id", "INTEGER"), ("s_geography_class", "VARCHAR"),
        ("s_market_desc", "VARCHAR"), ("s_market_manager", "VARCHAR"), ("s_division_id", "INTEGER"),
        ("s_division_name", "VARCHAR"), ("s_company_id", "INTEGER"), ("s_company_name", "VARCHAR"),
        ("s_street_number", "VARCHAR"), ("s_street_name", "VARCHAR"), ("s_street_type", "VARCHAR"),
        ("s_suite_number", "VARCHAR"), ("s_city", "VARCHAR"), ("s_county", "VARCHAR"),
        ("s_state", "VARCHAR"), ("s_zip", "VARCHAR"), ("s_country", "VARCHAR"),
        ("s_gmt_offset", "DECIMAL(5,2)"), ("s_tax_precentage", "DECIMAL(5,2)"),
    ],
    "store_returns": [
        ("sr_returned_date_sk", "INTEGER"), ("sr_return_time_sk", "INTEGER"), ("sr_item_sk", "INTEGER"),
        ("sr_customer_sk", "INTEGER"), ("sr_cdemo_sk", "INTEGER"), ("sr_hdemo_sk", "INTEGER"),
        ("sr_addr_sk", "INTEGER"), ("sr_store_sk", "INTEGER"), ("sr_reason_sk", "INTEGER"),
        ("sr_ticket_number", "BIGINT"), ("sr_return_quantity", "INTEGER"),
        ("sr_return_amt", "DECIMAL(7,2)"), ("sr_return_tax", "DECIMAL(7,2)"),
        ("sr_return_amt_inc_tax", "DECIMAL(7,2)"), ("sr_fee", "DECIMAL(7,2)"),
        ("sr_return_ship_cost", "DECIMAL(7,2)"), ("sr_refunded_cash", "DECIMAL(7,2)"),
        ("sr_reversed_charge", "DECIMAL(7,2)"), ("sr_store_credit", "DECIMAL(7,2)"),
        ("sr_net_loss", "DECIMAL(7,2)"),
    ],
    "store_sales": [
        ("ss_sold_date_sk", "INTEGER"), ("ss_sold_time_sk", "INTEGER"), ("ss_item_sk", "INTEGER"),
        ("ss_customer_sk", "INTEGER"), ("ss_cdemo_sk", "INTEGER"), ("ss_hdemo_sk", "INTEGER"),
        ("ss_addr_sk", "INTEGER"), ("ss_store_sk", "INTEGER"), ("ss_promo_sk", "INTEGER"),
        ("ss_ticket_number", "BIGINT"), ("ss_quantity", "INTEGER"),
        ("ss_wholesale_cost", "DECIMAL(7,2)"), ("ss_list_price", "DECIMAL(7,2)"),
        ("ss_sales_price", "DECIMAL(7,2)"), ("ss_ext_discount_amt", "DECIMAL(7,2)"),
        ("ss_ext_sales_price", "DECIMAL(7,2)"), ("ss_ext_wholesale_cost", "DECIMAL(7,2)"),
        ("ss_ext_list_price", "DECIMAL(7,2)"), ("ss_ext_tax", "DECIMAL(7,2)"),
        ("ss_coupon_amt", "DECIMAL(7,2)"), ("ss_net_paid", "DECIMAL(7,2)"),
        ("ss_net_paid_inc_tax", "DECIMAL(7,2)"), ("ss_net_profit", "DECIMAL(7,2)"),
    ],
    "time_dim": [
        ("t_time_sk", "INTEGER"), ("t_time_id", "VARCHAR"), ("t_time", "INTEGER"),
        ("t_hour", "INTEGER"), ("t_minute", "INTEGER"), ("t_second", "INTEGER"),
        ("t_am_pm", "VARCHAR"), ("t_shift", "VARCHAR"), ("t_sub_shift", "VARCHAR"),
        ("t_meal_time", "VARCHAR"),
    ],
    "warehouse": [
        ("w_warehouse_sk", "INTEGER"), ("w_warehouse_id", "VARCHAR"), ("w_warehouse_name", "VARCHAR"),
        ("w_warehouse_sq_ft", "INTEGER"), ("w_street_number", "VARCHAR"), ("w_street_name", "VARCHAR"),
        ("w_street_type", "VARCHAR"), ("w_suite_number", "VARCHAR"), ("w_city", "VARCHAR"),
        ("w_county", "VARCHAR"), ("w_state", "VARCHAR"), ("w_zip", "VARCHAR"),
        ("w_country", "VARCHAR"), ("w_gmt_offset", "DECIMAL(5,2)"),
    ],
    "web_page": [
        ("wp_web_page_sk", "INTEGER"), ("wp_web_page_id", "VARCHAR"), ("wp_rec_start_date", "DATE"),
        ("wp_rec_end_date", "DATE"), ("wp_creation_date_sk", "INTEGER"), ("wp_access_date_sk", "INTEGER"),
        ("wp_autogen_flag", "VARCHAR"), ("wp_customer_sk", "INTEGER"), ("wp_url", "VARCHAR"),
        ("wp_type", "VARCHAR"), ("wp_char_count", "INTEGER"), ("wp_link_count", "INTEGER"),
        ("wp_image_count", "INTEGER"), ("wp_max_ad_count", "INTEGER"),
    ],
    "web_returns": [
        ("wr_returned_date_sk", "INTEGER"), ("wr_returned_time_sk", "INTEGER"), ("wr_item_sk", "INTEGER"),
        ("wr_refunded_customer_sk", "INTEGER"), ("wr_refunded_cdemo_sk", "INTEGER"),
        ("wr_refunded_hdemo_sk", "INTEGER"), ("wr_refunded_addr_sk", "INTEGER"),
        ("wr_returning_customer_sk", "INTEGER"), ("wr_returning_cdemo_sk", "INTEGER"),
        ("wr_returning_hdemo_sk", "INTEGER"), ("wr_returning_addr_sk", "INTEGER"),
        ("wr_web_page_sk", "INTEGER"), ("wr_reason_sk", "INTEGER"), ("wr_order_number", "BIGINT"),
        ("wr_return_quantity", "INTEGER"), ("wr_return_amt", "DECIMAL(7,2)"),
        ("wr_return_tax", "DECIMAL(7,2)"), ("wr_return_amt_inc_tax", "DECIMAL(7,2)"),
        ("wr_fee", "DECIMAL(7,2)"), ("wr_return_ship_cost", "DECIMAL(7,2)"),
        ("wr_refunded_cash", "DECIMAL(7,2)"), ("wr_reversed_charge", "DECIMAL(7,2)"),
        ("wr_account_credit", "DECIMAL(7,2)"), ("wr_net_loss", "DECIMAL(7,2)"),
    ],
    "web_sales": [
        ("ws_sold_date_sk", "INTEGER"), ("ws_sold_time_sk", "INTEGER"), ("ws_ship_date_sk", "INTEGER"),
        ("ws_item_sk", "INTEGER"), ("ws_bill_customer_sk", "INTEGER"), ("ws_bill_cdemo_sk", "INTEGER"),
        ("ws_bill_hdemo_sk", "INTEGER"), ("ws_bill_addr_sk", "INTEGER"),
        ("ws_ship_customer_sk", "INTEGER"), ("ws_ship_cdemo_sk", "INTEGER"),
        ("ws_ship_hdemo_sk", "INTEGER"), ("ws_ship_addr_sk", "INTEGER"),
        ("ws_web_page_sk", "INTEGER"), ("ws_web_site_sk", "INTEGER"), ("ws_ship_mode_sk", "INTEGER"),
        ("ws_warehouse_sk", "INTEGER"), ("ws_promo_sk", "INTEGER"), ("ws_order_number", "BIGINT"),
        ("ws_quantity", "INTEGER"), ("ws_wholesale_cost", "DECIMAL(7,2)"),
        ("ws_list_price", "DECIMAL(7,2)"), ("ws_sales_price", "DECIMAL(7,2)"),
        ("ws_ext_discount_amt", "DECIMAL(7,2)"), ("ws_ext_sales_price", "DECIMAL(7,2)"),
        ("ws_ext_wholesale_cost", "DECIMAL(7,2)"), ("ws_ext_list_price", "DECIMAL(7,2)"),
        ("ws_ext_tax", "DECIMAL(7,2)"), ("ws_coupon_amt", "DECIMAL(7,2)"),
        ("ws_ext_ship_cost", "DECIMAL(7,2)"), ("ws_net_paid", "DECIMAL(7,2)"),
        ("ws_net_paid_inc_tax", "DECIMAL(7,2)"), ("ws_net_paid_inc_ship", "DECIMAL(7,2)"),
        ("ws_net_paid_inc_ship_tax", "DECIMAL(7,2)"), ("ws_net_profit", "DECIMAL(7,2)"),
    ],
    "web_site": [
        ("web_site_sk", "INTEGER"), ("web_site_id", "VARCHAR"), ("web_rec_start_date", "DATE"),
        ("web_rec_end_date", "DATE"), ("web_name", "VARCHAR"), ("web_open_date_sk", "INTEGER"),
        ("web_close_date_sk", "INTEGER"), ("web_class", "VARCHAR"), ("web_manager", "VARCHAR"),
        ("web_mkt_id", "INTEGER"), ("web_mkt_class", "VARCHAR"), ("web_mkt_desc", "VARCHAR"),
        ("web_market_manager", "VARCHAR"), ("web_company_id", "INTEGER"), ("web_company_name", "VARCHAR"),
        ("web_street_number", "VARCHAR"), ("web_street_name", "VARCHAR"), ("web_street_type", "VARCHAR"),
        ("web_suite_number", "VARCHAR"), ("web_city", "VARCHAR"), ("web_county", "VARCHAR"),
        ("web_state", "VARCHAR"), ("web_zip", "VARCHAR"), ("web_country", "VARCHAR"),
        ("web_gmt_offset", "DECIMAL(5,2)"), ("web_tax_percentage", "DECIMAL(5,2)"),
    ],
}


def convert_dat_to_parquet(dat_file: Path, table_name: str, output_dir: Path):
    """Convert a single .dat file to Parquet using DuckDB."""
    schema = TPCDS_SCHEMAS.get(table_name)
    if not schema:
        print(f"  WARNING: No schema for {table_name}, skipping")
        return None

    out_file = output_dir / f"{table_name}.parquet"
    if out_file.exists():
        size_mb = out_file.stat().st_size / (1024 * 1024)
        print(f"  {table_name}: already exists ({size_mb:.1f} MB)")
        return out_file

    # Build column definitions for DuckDB read_csv
    col_names = [c[0] for c in schema]
    col_types = {}
    for name, dtype in schema:
        # Map SQL types to DuckDB types
        if "DECIMAL" in dtype:
            col_types[name] = dtype
        elif dtype == "BIGINT":
            col_types[name] = "BIGINT"
        elif dtype == "INTEGER":
            col_types[name] = "INTEGER"
        elif dtype == "DATE":
            col_types[name] = "DATE"
        else:
            col_types[name] = "VARCHAR"

    # DuckDB: read pipe-delimited CSV with trailing delimiter
    # TPC-DS .dat files have a trailing | on each line
    db = duckdb.connect()

    # Build the columns spec
    cols_sql = ", ".join(
        f"column{i:02d} {col_types[name]}" for i, (name, _) in enumerate(schema)
    )

    num_cols = len(schema)
    # dsdgen adds trailing |, which creates an extra empty column
    # We read all columns + 1 dummy, then select only the real ones
    try:
        # Read with auto-detection disabled, explicit column count
        # The trailing | means there's an extra empty field at the end
        select_cols = ", ".join(f"column{i:02d} AS {col_names[i]}" for i in range(num_cols))

        db.execute(f"""
            COPY (
                SELECT {select_cols}
                FROM read_csv('{dat_file}',
                    delim='|',
                    header=false,
                    null_padding=true,
                    ignore_errors=true,
                    all_varchar=true)
            ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        size_mb = out_file.stat().st_size / (1024 * 1024)
        row_count = db.execute(f"SELECT COUNT(*) FROM '{out_file}'").fetchone()[0]
        print(f"  {table_name}: {row_count:,} rows, {size_mb:.1f} MB")
        db.close()
        return out_file

    except Exception as e:
        db.close()
        if out_file.exists():
            out_file.unlink()
        print(f"  {table_name}: FAILED - {e}")
        return None


def upload_to_minio(parquet_dir: Path, bucket: str, endpoint: str, access_key: str, secret_key: str):
    """Upload parquet files to MinIO using boto3 or mc CLI."""
    parquet_files = sorted(parquet_dir.glob("*.parquet"))
    if not parquet_files:
        print("No parquet files to upload!")
        return

    # Try boto3 first
    try:
        import boto3
        from botocore.client import Config as BotoConfig

        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=BotoConfig(signature_version="s3v4"),
        )

        # Create bucket if needed
        try:
            s3.head_bucket(Bucket=bucket)
        except Exception:
            s3.create_bucket(Bucket=bucket)
            print(f"  Created bucket: {bucket}")

        for pf in parquet_files:
            key = pf.name
            size_mb = pf.stat().st_size / (1024 * 1024)
            print(f"  Uploading {key} ({size_mb:.1f} MB)...", end=" ", flush=True)
            s3.upload_file(str(pf), bucket, key)
            print("OK")

        print(f"\n  All files uploaded to s3://{bucket}/")
        return

    except ImportError:
        print("  boto3 not available, falling back to mc CLI...")

    # Fallback: mc CLI
    mc_bin = SCRIPT_DIR / "firebolt-core" / "mc"
    if not mc_bin.exists():
        mc_bin = "mc"  # hope it's on PATH

    # Configure mc alias
    subprocess.run(
        [str(mc_bin), "alias", "set", "minio", endpoint, access_key, secret_key],
        capture_output=True,
    )

    # Create bucket
    subprocess.run(
        [str(mc_bin), "mb", "--ignore-existing", f"minio/{bucket}"],
        capture_output=True,
    )

    for pf in parquet_files:
        size_mb = pf.stat().st_size / (1024 * 1024)
        print(f"  Uploading {pf.name} ({size_mb:.1f} MB)...", end=" ", flush=True)
        result = subprocess.run(
            [str(mc_bin), "cp", str(pf), f"minio/{bucket}/{pf.name}"],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            print("OK")
        else:
            print(f"FAILED: {result.stderr[:200]}")

    print(f"\n  All files uploaded to s3://{bucket}/")


def main():
    p = argparse.ArgumentParser(description="Convert TPC-DS .dat to Parquet and upload to MinIO")
    p.add_argument("--scale", type=int, default=20, help="Scale factor (default: 20)")
    p.add_argument("--data-dir", type=str, default=None, help="Override data directory")
    p.add_argument("--upload", action="store_true", help="Upload to MinIO after conversion")
    p.add_argument("--bucket", default="tpcds", help="S3/MinIO bucket name (default: tpcds)")
    p.add_argument("--endpoint", default="http://localhost:9000", help="MinIO endpoint")
    p.add_argument("--access-key", default="minioadmin", help="MinIO access key")
    p.add_argument("--secret-key", default="minioadmin", help="MinIO secret key")
    args = p.parse_args()

    if args.data_dir:
        data_dir = Path(args.data_dir)
    else:
        data_dir = SCRIPT_DIR / f"tpcds-data/sf{args.scale}"

    parquet_dir = data_dir / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    print(f"TPC-DS .dat -> Parquet Converter (SF{args.scale})")
    print(f"========================================")
    print(f"Source: {data_dir}")
    print(f"Output: {parquet_dir}")
    print(f"========================================")

    # Convert each table
    print(f"\nStep 1: Converting .dat files to Parquet...")

    converted = 0
    for table_name in TPCDS_SCHEMAS:
        dat_file = data_dir / f"{table_name}.dat"
        if not dat_file.exists():
            continue
        result = convert_dat_to_parquet(dat_file, table_name, parquet_dir)
        if result:
            converted += 1

    print(f"\n  Converted {converted} tables")

    # Show total size
    total_size = sum(f.stat().st_size for f in parquet_dir.glob("*.parquet"))
    print(f"  Total Parquet size: {total_size / (1024**3):.2f} GB")

    # Upload if requested
    if args.upload:
        print(f"\nStep 2: Uploading to MinIO ({args.endpoint})...")
        upload_to_minio(parquet_dir, args.bucket, args.endpoint, args.access_key, args.secret_key)

    # Print Databend external scan SQL
    print(f"\n========================================")
    print(f"To query Parquet files directly from Databend:")
    print(f"========================================")
    print(f"""
-- Example: scan store_sales from MinIO
SELECT COUNT(*) FROM 's3://{args.bucket}/store_sales.parquet'
(CONNECTION => (
    ENDPOINT_URL = '{args.endpoint}',
    ACCESS_KEY_ID = '{args.access_key}',
    SECRET_ACCESS_KEY = '{args.secret_key}'
));

-- Example: aggregate scan
SELECT SUM(ss_net_paid) FROM 's3://{args.bucket}/store_sales.parquet'
(CONNECTION => (
    ENDPOINT_URL = '{args.endpoint}',
    ACCESS_KEY_ID = '{args.access_key}',
    SECRET_ACCESS_KEY = '{args.secret_key}'
));
""")


if __name__ == "__main__":
    main()
