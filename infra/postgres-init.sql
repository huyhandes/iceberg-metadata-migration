-- Shared postgres init: create databases and users for all services

-- Hive Metastore
CREATE USER hive WITH PASSWORD 'hive';
CREATE DATABASE metastore OWNER hive;
