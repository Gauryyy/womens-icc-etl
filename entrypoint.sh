#!/bin/sh

echo "Running ETL pipeline..."
python src/main.py

echo "Starting Flask app..."
python web/app.py