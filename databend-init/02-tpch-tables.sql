-- TPC-H Table Definitions for Databend
-- Creates tables in all TPCH databases (tpch500m, tpch1g, tpch5g, tpch9g)
-- Uses fully qualified table names to avoid session state issues

-- ============ tpch1g ============

CREATE TABLE IF NOT EXISTS tpch1g.nation (
    n_nationkey INT NOT NULL,
    n_name VARCHAR(25) NOT NULL,
    n_regionkey INT NOT NULL,
    n_comment VARCHAR(152)
);

CREATE TABLE IF NOT EXISTS tpch1g.region (
    r_regionkey INT NOT NULL,
    r_name VARCHAR(25) NOT NULL,
    r_comment VARCHAR(152)
);

CREATE TABLE IF NOT EXISTS tpch1g.part (
    p_partkey INT NOT NULL,
    p_name VARCHAR(55) NOT NULL,
    p_mfgr VARCHAR(25) NOT NULL,
    p_brand VARCHAR(10) NOT NULL,
    p_type VARCHAR(25) NOT NULL,
    p_size INT NOT NULL,
    p_container VARCHAR(10) NOT NULL,
    p_retailprice DECIMAL(15, 2) NOT NULL,
    p_comment VARCHAR(23) NOT NULL
);

CREATE TABLE IF NOT EXISTS tpch1g.supplier (
    s_suppkey INT NOT NULL,
    s_name VARCHAR(25) NOT NULL,
    s_address VARCHAR(40) NOT NULL,
    s_nationkey INT NOT NULL,
    s_phone VARCHAR(15) NOT NULL,
    s_acctbal DECIMAL(15, 2) NOT NULL,
    s_comment VARCHAR(101) NOT NULL
);

CREATE TABLE IF NOT EXISTS tpch1g.partsupp (
    ps_partkey INT NOT NULL,
    ps_suppkey INT NOT NULL,
    ps_availqty INT NOT NULL,
    ps_supplycost DECIMAL(15, 2) NOT NULL,
    ps_comment VARCHAR(199) NOT NULL
);

CREATE TABLE IF NOT EXISTS tpch1g.customer (
    c_custkey INT NOT NULL,
    c_name VARCHAR(25) NOT NULL,
    c_address VARCHAR(40) NOT NULL,
    c_nationkey INT NOT NULL,
    c_phone VARCHAR(15) NOT NULL,
    c_acctbal DECIMAL(15, 2) NOT NULL,
    c_mktsegment VARCHAR(10) NOT NULL,
    c_comment VARCHAR(117) NOT NULL
);

CREATE TABLE IF NOT EXISTS tpch1g.orders (
    o_orderkey BIGINT NOT NULL,
    o_custkey INT NOT NULL,
    o_orderstatus VARCHAR(1) NOT NULL,
    o_totalprice DECIMAL(15, 2) NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority VARCHAR(15) NOT NULL,
    o_clerk VARCHAR(15) NOT NULL,
    o_shippriority INT NOT NULL,
    o_comment VARCHAR(79) NOT NULL
);

CREATE TABLE IF NOT EXISTS tpch1g.lineitem (
    l_orderkey BIGINT NOT NULL,
    l_partkey INT NOT NULL,
    l_suppkey INT NOT NULL,
    l_linenumber INT NOT NULL,
    l_quantity DECIMAL(15, 2) NOT NULL,
    l_extendedprice DECIMAL(15, 2) NOT NULL,
    l_discount DECIMAL(15, 2) NOT NULL,
    l_tax DECIMAL(15, 2) NOT NULL,
    l_returnflag VARCHAR(1) NOT NULL,
    l_linestatus VARCHAR(1) NOT NULL,
    l_shipdate DATE NOT NULL,
    l_commitdate DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct VARCHAR(25) NOT NULL,
    l_shipmode VARCHAR(10) NOT NULL,
    l_comment VARCHAR(44) NOT NULL
);

-- ============ tpch500m ============

CREATE TABLE IF NOT EXISTS tpch500m.nation LIKE tpch1g.nation;
CREATE TABLE IF NOT EXISTS tpch500m.region LIKE tpch1g.region;
CREATE TABLE IF NOT EXISTS tpch500m.part LIKE tpch1g.part;
CREATE TABLE IF NOT EXISTS tpch500m.supplier LIKE tpch1g.supplier;
CREATE TABLE IF NOT EXISTS tpch500m.partsupp LIKE tpch1g.partsupp;
CREATE TABLE IF NOT EXISTS tpch500m.customer LIKE tpch1g.customer;
CREATE TABLE IF NOT EXISTS tpch500m.orders LIKE tpch1g.orders;
CREATE TABLE IF NOT EXISTS tpch500m.lineitem LIKE tpch1g.lineitem;

-- ============ tpch5g ============

CREATE TABLE IF NOT EXISTS tpch5g.nation LIKE tpch1g.nation;
CREATE TABLE IF NOT EXISTS tpch5g.region LIKE tpch1g.region;
CREATE TABLE IF NOT EXISTS tpch5g.part LIKE tpch1g.part;
CREATE TABLE IF NOT EXISTS tpch5g.supplier LIKE tpch1g.supplier;
CREATE TABLE IF NOT EXISTS tpch5g.partsupp LIKE tpch1g.partsupp;
CREATE TABLE IF NOT EXISTS tpch5g.customer LIKE tpch1g.customer;
CREATE TABLE IF NOT EXISTS tpch5g.orders LIKE tpch1g.orders;
CREATE TABLE IF NOT EXISTS tpch5g.lineitem LIKE tpch1g.lineitem;

-- ============ tpch9g ============

CREATE TABLE IF NOT EXISTS tpch9g.nation LIKE tpch1g.nation;
CREATE TABLE IF NOT EXISTS tpch9g.region LIKE tpch1g.region;
CREATE TABLE IF NOT EXISTS tpch9g.part LIKE tpch1g.part;
CREATE TABLE IF NOT EXISTS tpch9g.supplier LIKE tpch1g.supplier;
CREATE TABLE IF NOT EXISTS tpch9g.partsupp LIKE tpch1g.partsupp;
CREATE TABLE IF NOT EXISTS tpch9g.customer LIKE tpch1g.customer;
CREATE TABLE IF NOT EXISTS tpch9g.orders LIKE tpch1g.orders;
CREATE TABLE IF NOT EXISTS tpch9g.lineitem LIKE tpch1g.lineitem;
