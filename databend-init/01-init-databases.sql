-- Initialize databases for PBench testing
-- These databases correspond to the ones referenced in the config files

-- TPCH databases (different sizes)
CREATE DATABASE IF NOT EXISTS tpch500m;
CREATE DATABASE IF NOT EXISTS tpch1g;
CREATE DATABASE IF NOT EXISTS tpch5g;
CREATE DATABASE IF NOT EXISTS tpch9g;

-- TPCDS databases
CREATE DATABASE IF NOT EXISTS tpcds1g;
CREATE DATABASE IF NOT EXISTS tpcds2g;

-- IMDB database
CREATE DATABASE IF NOT EXISTS imdb;

-- LLM database
CREATE DATABASE IF NOT EXISTS llm;

-- Create a simple test table in each database for testing
USE tpch500m;
CREATE TABLE IF NOT EXISTS test_table (id INT, value VARCHAR);
INSERT INTO test_table VALUES (1, 'test');

USE tpch1g;
CREATE TABLE IF NOT EXISTS test_table (id INT, value VARCHAR);

USE tpch5g;
CREATE TABLE IF NOT EXISTS test_table (id INT, value VARCHAR);

USE tpch9g;
CREATE TABLE IF NOT EXISTS test_table (id INT, value VARCHAR);

USE tpcds1g;
CREATE TABLE IF NOT EXISTS test_table (id INT, value VARCHAR);

USE tpcds2g;
CREATE TABLE IF NOT EXISTS test_table (id INT, value VARCHAR);

USE imdb;
CREATE TABLE IF NOT EXISTS test_table (id INT, value VARCHAR);

USE llm;
CREATE TABLE IF NOT EXISTS test_table (id INT, value VARCHAR);
