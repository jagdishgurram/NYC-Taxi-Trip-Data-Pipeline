# NYC Taxi Data Engineering Pipeline

End-to-End Data Pipeline using Medallion Architecture

---

## Overview
The data architecture for this project follows Medallion Architecture. \
**Bronze** → **Silver** → **Gold** \
Raw taxi trip data is ingested, cleaned, transformed into analytical tables, \
and used to perform trip analysis and business insights.

---

## Tech Stack
- Python
- SQL
- PySpark
- VS Code

---

## Data Architecture
![Data Architecture]()

**Bronze Layer** \
Raw taxi trip data ingested from source files.

**Silver Layer** \
Cleaned and validated trip data.

**Gold Layer** \
Star schema tables optimized for analytics.

---

## Pipeline Workflow 

1. Load raw taxi data into Bronze layer
2. Clean and validate data in Silver layer
3. Create star schema tables in Gold layer
4. Run analytics queries for insights

---

## Sample Insights
- Top pickup locations \
![Sample Insights](docs/trip_analysis.png)
- Average trip distance \
![Sample Insights](docs/average_trip_distance.png)

---

## Key Insights
- Manhattan borough Generates the Highest Revenue
- Average trip distance is around 2.8 miles
- Upper East Side South is the Popular Pickup Zone
---

### DataSet Used | NYC Taxi Trip Dataset | [Download Link](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-01.parquet)
---

## Project Structure
```
NYC-Taxi-Trip-Data-Pipeline/
│
├── datasets/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│ 
├── etl/
│   ├── transform/
│   ├── load/
│ 
├── analytics/
│   ├── final_trip_analysis/
│ 
├── notebooks/
│   ├── eda/
│ 
├── docs/
├── README.md
├── .gitignore
├── LICENSE
└── requirements.txt 
```
---
## Future Improvements
- Automate pipeline using Airflow
- Store data in a cloud data warehouse
- Build dashboard using BI tools
---
## License
This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.
