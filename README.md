# mysql-stock-data

This project has three files

CreateTable.py: Creates the schemas and tables for the stock database architecture
Populate_Ticker_Dir.py: Creates the tables with stock symbols. Each symbol has a unique Stock_ID
multi_proc_v2.py: Using MySQL's LOAD DATA INFILE uploads all Stock data CSV files to their respective daily-ticker-price table.
