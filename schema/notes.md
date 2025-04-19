# Notes: Data Modeling Summary

## Project: `dwh-taxi-sandbox`

This project aims to simulate a full analytical data warehouse based on NYC taxi data, extended with synthetic dimensions to support a normalized **snowflake schema**.  
The model is intended for learning purposes, with orchestration planned via Apache Airflow and storage in a relational SQL database.

- The database schema is normalized to **Third Normal Form (3NF)**.
- `/schema/schema_v1.png` — ER diagram (exported from dbdiagram.io)
- `/schema/dwh_schema.dbml` — Logical schema in DBML format
