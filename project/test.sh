#!/bin/bash

# Define paths
DATA_DIR='data'
CSV_PATH="$DATA_DIR/jail_deaths.csv"
DB_PATH="$DATA_DIR/jail_deaths.db"
LOG_FILE="$DATA_DIR/pipeline_log.txt"

# Run the data pipeline
echo "Running data pipeline..."
python3 main.py

# Check if the CSV file is created
if [ -f "$CSV_PATH" ]; then
    echo "CSV file created successfully."
else
    echo "CSV file not found. Test failed."
    exit 1
fi

# Check if the SQLite database file is created
if [ -f "$DB_PATH" ]; then
    echo "SQLite database file created successfully."
else
    echo "SQLite database file not found. Test failed."
    exit 1
fi

# Check if the log file is created
if [ -f "$LOG_FILE" ]; then
    echo "Log file created successfully."
else
    echo "Log file not found. Test failed."
    exit 1
fi

echo "All tests passed successfully."