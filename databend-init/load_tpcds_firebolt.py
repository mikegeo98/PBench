#!/usr/bin/env python3
"""
Load TPC-DS data into Firebolt-Core.

This script expects pre-generated TPC-DS .dat files in:
  databend-init/tpcds-data/sf<SCALE>/

Usage:
    python load_tpcds_firebolt.py [scale_factor] [database] [host] [port]

Examples:
    python load_tpcds_firebolt.py 1 tpch1g
    python load_tpcds_firebolt.py 1 tpch1g localhost 3473
"""

from __future__ import annotations

import sys
import time
import re
from pathlib import Path

import requests


# TPC-DS tables in dependency order
TABLES = [
    # Dimension tables
    "call_center",
    "catalog_page",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "time_dim",
    "warehouse",
    "web_page",
    "web_site",
    "customer",
    # Fact tables
    "inventory",
    "store_sales",
    "store_returns",
    "catalog_sales",
    "catalog_returns",
    "web_sales",
    "web_returns",
]

CREATE_STATEMENTS = [
"""CREATE TABLE call_center (
    cc_call_center_sk INTEGER NOT NULL,
    cc_call_center_id VARCHAR(16) NOT NULL,
    cc_rec_start_date DATE,
    cc_rec_end_date DATE,
    cc_closed_date_sk INTEGER,
    cc_open_date_sk INTEGER,
    cc_name VARCHAR(50),
    cc_class VARCHAR(50),
    cc_employees INTEGER,
    cc_sq_ft INTEGER,
    cc_hours VARCHAR(20),
    cc_manager VARCHAR(40),
    cc_mkt_id INTEGER,
    cc_mkt_class VARCHAR(50),
    cc_mkt_desc VARCHAR(100),
    cc_market_manager VARCHAR(40),
    cc_division INTEGER,
    cc_division_name VARCHAR(50),
    cc_company INTEGER,
    cc_company_name VARCHAR(50),
    cc_street_number VARCHAR(10),
    cc_street_name VARCHAR(60),
    cc_street_type VARCHAR(15),
    cc_suite_number VARCHAR(10),
    cc_city VARCHAR(60),
    cc_county VARCHAR(30),
    cc_state VARCHAR(2),
    cc_zip VARCHAR(10),
    cc_country VARCHAR(20),
    cc_gmt_offset DECIMAL(5,2),
    cc_tax_percentage DECIMAL(5,2)
) PRIMARY INDEX cc_call_center_sk;""",
"""CREATE TABLE catalog_page (
    cp_catalog_page_sk INTEGER NOT NULL,
    cp_catalog_page_id VARCHAR(16) NOT NULL,
    cp_start_date_sk INTEGER,
    cp_end_date_sk INTEGER,
    cp_department VARCHAR(50),
    cp_catalog_number INTEGER,
    cp_catalog_page_number INTEGER,
    cp_description VARCHAR(100),
    cp_type VARCHAR(100)
) PRIMARY INDEX cp_catalog_page_sk;""",
"""CREATE TABLE customer_address (
    ca_address_sk INTEGER NOT NULL,
    ca_address_id VARCHAR(16) NOT NULL,
    ca_street_number VARCHAR(10),
    ca_street_name VARCHAR(60),
    ca_street_type VARCHAR(15),
    ca_suite_number VARCHAR(10),
    ca_city VARCHAR(60),
    ca_county VARCHAR(30),
    ca_state VARCHAR(2),
    ca_zip VARCHAR(10),
    ca_country VARCHAR(20),
    ca_gmt_offset DECIMAL(5,2),
    ca_location_type VARCHAR(20)
) PRIMARY INDEX ca_address_sk;""",
"""CREATE TABLE customer_demographics (
    cd_demo_sk INTEGER NOT NULL,
    cd_gender VARCHAR(1),
    cd_marital_status VARCHAR(1),
    cd_education_status VARCHAR(20),
    cd_purchase_estimate INTEGER,
    cd_credit_rating VARCHAR(10),
    cd_dep_count INTEGER,
    cd_dep_employed_count INTEGER,
    cd_dep_college_count INTEGER
) PRIMARY INDEX cd_demo_sk;""",
"""CREATE TABLE date_dim (
    d_date_sk INTEGER NOT NULL,
    d_date_id VARCHAR(16) NOT NULL,
    d_date DATE,
    d_month_seq INTEGER,
    d_week_seq INTEGER,
    d_quarter_seq INTEGER,
    d_year INTEGER,
    d_dow INTEGER,
    d_moy INTEGER,
    d_dom INTEGER,
    d_qoy INTEGER,
    d_fy_year INTEGER,
    d_fy_quarter_seq INTEGER,
    d_fy_week_seq INTEGER,
    d_day_name VARCHAR(9),
    d_quarter_name VARCHAR(6),
    d_holiday VARCHAR(1),
    d_weekend VARCHAR(1),
    d_following_holiday VARCHAR(1),
    d_first_dom INTEGER,
    d_last_dom INTEGER,
    d_same_day_ly INTEGER,
    d_same_day_lq INTEGER,
    d_current_day VARCHAR(1),
    d_current_week VARCHAR(1),
    d_current_month VARCHAR(1),
    d_current_quarter VARCHAR(1),
    d_current_year VARCHAR(1)
) PRIMARY INDEX d_date_sk;""",
"""CREATE TABLE household_demographics (
    hd_demo_sk INTEGER NOT NULL,
    hd_income_band_sk INTEGER,
    hd_buy_potential VARCHAR(15),
    hd_dep_count INTEGER,
    hd_vehicle_count INTEGER
) PRIMARY INDEX hd_demo_sk;""",
"""CREATE TABLE income_band (
    ib_income_band_sk INTEGER NOT NULL,
    ib_lower_bound INTEGER,
    ib_upper_bound INTEGER
) PRIMARY INDEX ib_income_band_sk;""",
"""CREATE TABLE item (
    i_item_sk INTEGER NOT NULL,
    i_item_id VARCHAR(16) NOT NULL,
    i_rec_start_date DATE,
    i_rec_end_date DATE,
    i_item_desc VARCHAR(200),
    i_current_price DECIMAL(15,2),
    i_wholesale_cost DECIMAL(15,2),
    i_brand_id INTEGER,
    i_brand VARCHAR(50),
    i_class_id INTEGER,
    i_class VARCHAR(50),
    i_category_id INTEGER,
    i_category VARCHAR(50),
    i_manufact_id INTEGER,
    i_manufact VARCHAR(50),
    i_size VARCHAR(20),
    i_formulation VARCHAR(20),
    i_color VARCHAR(20),
    i_units VARCHAR(10),
    i_container VARCHAR(10),
    i_manager_id INTEGER,
    i_product_name VARCHAR(50)
) PRIMARY INDEX i_item_sk;""",
"""CREATE TABLE promotion (
    p_promo_sk INTEGER NOT NULL,
    p_promo_id VARCHAR(16) NOT NULL,
    p_start_date_sk INTEGER,
    p_end_date_sk INTEGER,
    p_item_sk INTEGER,
    p_cost DECIMAL(15,2),
    p_response_target INTEGER,
    p_promo_name VARCHAR(50),
    p_channel_dmail VARCHAR(1),
    p_channel_email VARCHAR(1),
    p_channel_catalog VARCHAR(1),
    p_channel_tv VARCHAR(1),
    p_channel_radio VARCHAR(1),
    p_channel_press VARCHAR(1),
    p_channel_event VARCHAR(1),
    p_channel_demo VARCHAR(1),
    p_channel_details VARCHAR(100),
    p_purpose VARCHAR(15),
    p_discount_active VARCHAR(1)
) PRIMARY INDEX p_promo_sk;""",
"""CREATE TABLE reason (
    r_reason_sk INTEGER NOT NULL,
    r_reason_id VARCHAR(16) NOT NULL,
    r_reason_desc VARCHAR(100)
) PRIMARY INDEX r_reason_sk;""",
"""CREATE TABLE ship_mode (
    sm_ship_mode_sk INTEGER NOT NULL,
    sm_ship_mode_id VARCHAR(16) NOT NULL,
    sm_type VARCHAR(30),
    sm_code VARCHAR(10),
    sm_carrier VARCHAR(20),
    sm_contract VARCHAR(20)
) PRIMARY INDEX sm_ship_mode_sk;""",
"""CREATE TABLE store (
    s_store_sk INTEGER NOT NULL,
    s_store_id VARCHAR(16) NOT NULL,
    s_rec_start_date DATE,
    s_rec_end_date DATE,
    s_closed_date_sk INTEGER,
    s_store_name VARCHAR(50),
    s_number_employees INTEGER,
    s_floor_space INTEGER,
    s_hours VARCHAR(20),
    s_manager VARCHAR(40),
    s_market_id INTEGER,
    s_geography_class VARCHAR(100),
    s_market_desc VARCHAR(100),
    s_market_manager VARCHAR(40),
    s_division_id INTEGER,
    s_division_name VARCHAR(50),
    s_company_id INTEGER,
    s_company_name VARCHAR(50),
    s_street_number VARCHAR(10),
    s_street_name VARCHAR(60),
    s_street_type VARCHAR(15),
    s_suite_number VARCHAR(10),
    s_city VARCHAR(60),
    s_county VARCHAR(30),
    s_state VARCHAR(2),
    s_zip VARCHAR(10),
    s_country VARCHAR(20),
    s_gmt_offset DECIMAL(5,2),
    s_tax_precentage DECIMAL(5,2)
) PRIMARY INDEX s_store_sk;""",
"""CREATE TABLE time_dim (
    t_time_sk INTEGER NOT NULL,
    t_time_id VARCHAR(16) NOT NULL,
    t_time INTEGER,
    t_hour INTEGER,
    t_minute INTEGER,
    t_second INTEGER,
    t_am_pm VARCHAR(2),
    t_shift VARCHAR(20),
    t_sub_shift VARCHAR(20),
    t_meal_time VARCHAR(20)
) PRIMARY INDEX t_time_sk;""",
"""CREATE TABLE warehouse (
    w_warehouse_sk INTEGER NOT NULL,
    w_warehouse_id VARCHAR(16) NOT NULL,
    w_warehouse_name VARCHAR(20),
    w_warehouse_sq_ft INTEGER,
    w_street_number VARCHAR(10),
    w_street_name VARCHAR(60),
    w_street_type VARCHAR(15),
    w_suite_number VARCHAR(10),
    w_city VARCHAR(60),
    w_county VARCHAR(30),
    w_state VARCHAR(2),
    w_zip VARCHAR(10),
    w_country VARCHAR(20),
    w_gmt_offset DECIMAL(5,2)
) PRIMARY INDEX w_warehouse_sk;""",
"""CREATE TABLE web_page (
    wp_web_page_sk INTEGER NOT NULL,
    wp_web_page_id VARCHAR(16) NOT NULL,
    wp_rec_start_date DATE,
    wp_rec_end_date DATE,
    wp_creation_date_sk INTEGER,
    wp_access_date_sk INTEGER,
    wp_autogen_flag VARCHAR(1),
    wp_customer_sk INTEGER,
    wp_url VARCHAR(100),
    wp_type VARCHAR(50),
    wp_char_count INTEGER,
    wp_link_count INTEGER,
    wp_image_count INTEGER,
    wp_max_ad_count INTEGER
) PRIMARY INDEX wp_web_page_sk;""",
"""CREATE TABLE web_site (
    web_site_sk INTEGER NOT NULL,
    web_site_id VARCHAR(16) NOT NULL,
    web_rec_start_date DATE,
    web_rec_end_date DATE,
    web_name VARCHAR(50),
    web_open_date_sk INTEGER,
    web_close_date_sk INTEGER,
    web_class VARCHAR(50),
    web_manager VARCHAR(40),
    web_mkt_id INTEGER,
    web_mkt_class VARCHAR(50),
    web_mkt_desc VARCHAR(100),
    web_market_manager VARCHAR(40),
    web_company_id INTEGER,
    web_company_name VARCHAR(50),
    web_street_number VARCHAR(10),
    web_street_name VARCHAR(60),
    web_street_type VARCHAR(15),
    web_suite_number VARCHAR(10),
    web_city VARCHAR(60),
    web_county VARCHAR(30),
    web_state VARCHAR(2),
    web_zip VARCHAR(10),
    web_country VARCHAR(20),
    web_gmt_offset DECIMAL(5,2),
    web_tax_percentage DECIMAL(5,2)
) PRIMARY INDEX web_site_sk;""",
"""CREATE TABLE customer (
    c_customer_sk INTEGER NOT NULL,
    c_customer_id VARCHAR(16) NOT NULL,
    c_current_cdemo_sk INTEGER,
    c_current_hdemo_sk INTEGER,
    c_current_addr_sk INTEGER,
    c_first_shipto_date_sk INTEGER,
    c_first_sales_date_sk INTEGER,
    c_salutation VARCHAR(10),
    c_first_name VARCHAR(20),
    c_last_name VARCHAR(30),
    c_preferred_cust_flag VARCHAR(1),
    c_birth_day INTEGER,
    c_birth_month INTEGER,
    c_birth_year INTEGER,
    c_birth_country VARCHAR(20),
    c_login VARCHAR(13),
    c_email_address VARCHAR(50),
    c_last_review_date_sk INTEGER
) PRIMARY INDEX c_customer_sk;""",
"""CREATE TABLE inventory (
    inv_date_sk INTEGER NOT NULL,
    inv_item_sk INTEGER NOT NULL,
    inv_warehouse_sk INTEGER NOT NULL,
    inv_quantity_on_hand INTEGER
) PRIMARY INDEX inv_date_sk, inv_item_sk, inv_warehouse_sk;""",
"""CREATE TABLE store_sales (
    ss_sold_date_sk INTEGER,
    ss_sold_time_sk INTEGER,
    ss_item_sk INTEGER NOT NULL,
    ss_customer_sk INTEGER,
    ss_cdemo_sk INTEGER,
    ss_hdemo_sk INTEGER,
    ss_addr_sk INTEGER,
    ss_store_sk INTEGER,
    ss_promo_sk INTEGER,
    ss_ticket_number BIGINT NOT NULL,
    ss_quantity INTEGER,
    ss_wholesale_cost DECIMAL(15,2),
    ss_list_price DECIMAL(15,2),
    ss_sales_price DECIMAL(15,2),
    ss_ext_discount_amt DECIMAL(15,2),
    ss_ext_sales_price DECIMAL(15,2),
    ss_ext_wholesale_cost DECIMAL(15,2),
    ss_ext_list_price DECIMAL(15,2),
    ss_ext_tax DECIMAL(15,2),
    ss_coupon_amt DECIMAL(15,2),
    ss_net_paid DECIMAL(15,2),
    ss_net_paid_inc_tax DECIMAL(15,2),
    ss_net_profit DECIMAL(15,2)
) PRIMARY INDEX ss_item_sk, ss_ticket_number;""",
"""CREATE TABLE store_returns (
    sr_returned_date_sk INTEGER,
    sr_return_time_sk INTEGER,
    sr_item_sk INTEGER NOT NULL,
    sr_customer_sk INTEGER,
    sr_cdemo_sk INTEGER,
    sr_hdemo_sk INTEGER,
    sr_addr_sk INTEGER,
    sr_store_sk INTEGER,
    sr_reason_sk INTEGER,
    sr_ticket_number BIGINT NOT NULL,
    sr_return_quantity INTEGER,
    sr_return_amt DECIMAL(15,2),
    sr_return_tax DECIMAL(15,2),
    sr_return_amt_inc_tax DECIMAL(15,2),
    sr_fee DECIMAL(15,2),
    sr_return_ship_cost DECIMAL(15,2),
    sr_refunded_cash DECIMAL(15,2),
    sr_reversed_charge DECIMAL(15,2),
    sr_store_credit DECIMAL(15,2),
    sr_net_loss DECIMAL(15,2)
) PRIMARY INDEX sr_item_sk, sr_ticket_number;""",
"""CREATE TABLE catalog_sales (
    cs_sold_date_sk INTEGER,
    cs_sold_time_sk INTEGER,
    cs_ship_date_sk INTEGER,
    cs_bill_customer_sk INTEGER,
    cs_bill_cdemo_sk INTEGER,
    cs_bill_hdemo_sk INTEGER,
    cs_bill_addr_sk INTEGER,
    cs_ship_customer_sk INTEGER,
    cs_ship_cdemo_sk INTEGER,
    cs_ship_hdemo_sk INTEGER,
    cs_ship_addr_sk INTEGER,
    cs_call_center_sk INTEGER,
    cs_catalog_page_sk INTEGER,
    cs_ship_mode_sk INTEGER,
    cs_warehouse_sk INTEGER,
    cs_item_sk INTEGER NOT NULL,
    cs_promo_sk INTEGER,
    cs_order_number BIGINT NOT NULL,
    cs_quantity INTEGER,
    cs_wholesale_cost DECIMAL(15,2),
    cs_list_price DECIMAL(15,2),
    cs_sales_price DECIMAL(15,2),
    cs_ext_discount_amt DECIMAL(15,2),
    cs_ext_sales_price DECIMAL(15,2),
    cs_ext_wholesale_cost DECIMAL(15,2),
    cs_ext_list_price DECIMAL(15,2),
    cs_ext_tax DECIMAL(15,2),
    cs_coupon_amt DECIMAL(15,2),
    cs_ext_ship_cost DECIMAL(15,2),
    cs_net_paid DECIMAL(15,2),
    cs_net_paid_inc_tax DECIMAL(15,2),
    cs_net_paid_inc_ship DECIMAL(15,2),
    cs_net_paid_inc_ship_tax DECIMAL(15,2),
    cs_net_profit DECIMAL(15,2)
) PRIMARY INDEX cs_item_sk, cs_order_number;""",
"""CREATE TABLE catalog_returns (
    cr_returned_date_sk INTEGER,
    cr_returned_time_sk INTEGER,
    cr_item_sk INTEGER NOT NULL,
    cr_refunded_customer_sk INTEGER,
    cr_refunded_cdemo_sk INTEGER,
    cr_refunded_hdemo_sk INTEGER,
    cr_refunded_addr_sk INTEGER,
    cr_returning_customer_sk INTEGER,
    cr_returning_cdemo_sk INTEGER,
    cr_returning_hdemo_sk INTEGER,
    cr_returning_addr_sk INTEGER,
    cr_call_center_sk INTEGER,
    cr_catalog_page_sk INTEGER,
    cr_ship_mode_sk INTEGER,
    cr_warehouse_sk INTEGER,
    cr_reason_sk INTEGER,
    cr_order_number BIGINT NOT NULL,
    cr_return_quantity INTEGER,
    cr_return_amount DECIMAL(15,2),
    cr_return_tax DECIMAL(15,2),
    cr_return_amt_inc_tax DECIMAL(15,2),
    cr_fee DECIMAL(15,2),
    cr_return_ship_cost DECIMAL(15,2),
    cr_refunded_cash DECIMAL(15,2),
    cr_reversed_charge DECIMAL(15,2),
    cr_store_credit DECIMAL(15,2),
    cr_net_loss DECIMAL(15,2)
) PRIMARY INDEX cr_item_sk, cr_order_number;""",
"""CREATE TABLE web_sales (
    ws_sold_date_sk INTEGER,
    ws_sold_time_sk INTEGER,
    ws_ship_date_sk INTEGER,
    ws_item_sk INTEGER NOT NULL,
    ws_bill_customer_sk INTEGER,
    ws_bill_cdemo_sk INTEGER,
    ws_bill_hdemo_sk INTEGER,
    ws_bill_addr_sk INTEGER,
    ws_ship_customer_sk INTEGER,
    ws_ship_cdemo_sk INTEGER,
    ws_ship_hdemo_sk INTEGER,
    ws_ship_addr_sk INTEGER,
    ws_web_page_sk INTEGER,
    ws_web_site_sk INTEGER,
    ws_ship_mode_sk INTEGER,
    ws_warehouse_sk INTEGER,
    ws_promo_sk INTEGER,
    ws_order_number BIGINT NOT NULL,
    ws_quantity INTEGER,
    ws_wholesale_cost DECIMAL(15,2),
    ws_list_price DECIMAL(15,2),
    ws_sales_price DECIMAL(15,2),
    ws_ext_discount_amt DECIMAL(15,2),
    ws_ext_sales_price DECIMAL(15,2),
    ws_ext_wholesale_cost DECIMAL(15,2),
    ws_ext_list_price DECIMAL(15,2),
    ws_ext_tax DECIMAL(15,2),
    ws_coupon_amt DECIMAL(15,2),
    ws_ext_ship_cost DECIMAL(15,2),
    ws_net_paid DECIMAL(15,2),
    ws_net_paid_inc_tax DECIMAL(15,2),
    ws_net_paid_inc_ship DECIMAL(15,2),
    ws_net_paid_inc_ship_tax DECIMAL(15,2),
    ws_net_profit DECIMAL(15,2)
) PRIMARY INDEX ws_item_sk, ws_order_number;""",
"""CREATE TABLE web_returns (
    wr_returned_date_sk INTEGER,
    wr_returned_time_sk INTEGER,
    wr_item_sk INTEGER NOT NULL,
    wr_refunded_customer_sk INTEGER,
    wr_refunded_cdemo_sk INTEGER,
    wr_refunded_hdemo_sk INTEGER,
    wr_refunded_addr_sk INTEGER,
    wr_returning_customer_sk INTEGER,
    wr_returning_cdemo_sk INTEGER,
    wr_returning_hdemo_sk INTEGER,
    wr_returning_addr_sk INTEGER,
    wr_web_page_sk INTEGER,
    wr_reason_sk INTEGER,
    wr_order_number BIGINT NOT NULL,
    wr_return_quantity INTEGER,
    wr_return_amt DECIMAL(15,2),
    wr_return_tax DECIMAL(15,2),
    wr_return_amt_inc_tax DECIMAL(15,2),
    wr_fee DECIMAL(15,2),
    wr_return_ship_cost DECIMAL(15,2),
    wr_refunded_cash DECIMAL(15,2),
    wr_reversed_charge DECIMAL(15,2),
    wr_account_credit DECIMAL(15,2),
    wr_net_loss DECIMAL(15,2)
) PRIMARY INDEX wr_item_sk, wr_order_number;"""
]

EXPECTED_SF1 = {
    "call_center": 6,
    "catalog_page": 11718,
    "customer_address": 50000,
    "customer_demographics": 1920800,
    "date_dim": 73049,
    "household_demographics": 7200,
    "income_band": 20,
    "item": 18000,
    "promotion": 300,
    "reason": 35,
    "ship_mode": 20,
    "store": 12,
    "time_dim": 86400,
    "warehouse": 5,
    "web_page": 60,
    "web_site": 30,
    "customer": 100000,
    "inventory": 11745000,
    "store_sales": 2880404,
    "store_returns": 287999,
    "catalog_sales": 1441548,
    "catalog_returns": 144067,
    "web_sales": 719384,
    "web_returns": 71763,
}


def die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    sys.exit(code)


def run_sql(api_url: str, sql: str) -> str:
    response = requests.post(api_url, data=sql.encode("utf-8"), timeout=300)
    if response.status_code >= 400:
        die(f"ERROR running SQL: {response.status_code}\n{response.text.strip()}")
    return response.text.strip()


def count_rows(api_url: str, table: str) -> int:
    raw = run_sql(api_url, f"SELECT COUNT(*) FROM {table};")
    for line in raw.splitlines():
        cleaned = line.strip().replace(",", "")
        if re.fullmatch(r"\d+", cleaned):
            return int(cleaned)
    die(f"ERROR: unexpected count result for table {table}: {raw}")

def human_size(path: Path) -> str:
    size = path.stat().st_size
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            return f"{value:.1f}{unit}" if unit != "B" else f"{int(value)}{unit}"
        value /= 1024.0
    return f"{size}B"


def main() -> None:
    scale = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    database = sys.argv[2] if len(sys.argv) > 2 else "tpcds1g"
    host = sys.argv[3] if len(sys.argv) > 3 else "localhost"
    port = int(sys.argv[4]) if len(sys.argv) > 4 else 3473

    script_dir = Path(__file__).resolve().parent
    data_dir = script_dir / "tpcds-data" / f"sf{scale}"
    api_url = f"http://{host}:{port}/?output_format=psql&database={database}"

    print("TPC-DS Firebolt-Core Data Loader")
    print("========================================")
    print(f"Scale Factor: {scale}")
    print(f"Database: {database}")
    print(f"SQL API: {host}:{port}")
    print(f"Data dir: {data_dir}")
    print("========================================")

    # Note: local file check removed — COPY loads from S3 (MinIO), not local files

    print("\nStep 1: Creating database...")
    run_sql(api_url, f"CREATE DATABASE IF NOT EXISTS {database};")

    print("\nStep 2: Creating TPC-DS schema...")
    for statement in CREATE_STATEMENTS:
        run_sql(api_url, statement)
    print("  Schema created")

    print("\nStep 3: Loading data...")
    for table in TABLES:
        start = time.time()
        run_sql(api_url, f"COPY {table} FROM 's3://tpcds/sf{scale}/{table}.dat' WITH(CREDENTIALS=(AWS_ACCESS_KEY_ID ='minioadmin',AWS_SECRET_ACCESS_KEY = 'minioadmin'),HEADER = FALSE, TYPE = csv, DELIMITER='|');")
        elapsed = time.time() - start
        count = count_rows(api_url, table)
        print(f"{count} rows in {elapsed:.3f}s")

    print("\nStep 4: Verifying row counts...")
    total = 0
    for table in TABLES:
        count = count_rows(api_url, table)
        expected = EXPECTED_SF1[table] * scale
        status = "OK" if count == expected else f"(expected {expected})"
        print(f"  {table + ':':12} {count:>12,} {status}")
        total += count

    print(f"\n  {'TOTAL:':12} {total:>12,}")

    print("\nStep 5: Creating indexes...")
    print("  Index creation skipped (unsupported)")

    print("\n========================================")
    print("Done!")


if __name__ == "__main__":
    main()
