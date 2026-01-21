# Slovak Household Energy Consumption Forecasting

A data science pipeline for analyzing and forecasting residential energy consumption in Slovakia, developed for the KPMG Data Science Challenge.

## 🎯 Project Overview

This project builds a forecasting system that enables network providers to predict household energy consumption 3-24 hours ahead. Key capabilities include:

- **Behavioral Clustering**: Segment 1,000 households into 4 distinct consumption profiles
- **Consumption Forecasting**: XGBoost-based prediction with ~0.88 kWh MAE
- **Cluster-Level Forecasting**: Aggregate predictions for grid management decisions
- **Interactive Dashboard**: Flask web application for visualization (Docker-ready)

## 📊 Key Results

| Metric | Value |
|--------|-------|
| Households analyzed | 1,000 |
| Data points | 9.5M hourly records |
| Forecasting RMSE | 1.93 kWh |
| Forecasting MAE | 0.88 kWh |
| Clustering (K=4) | Silhouette Score 0.31 |

### Cluster Profiles

| Cluster | Profile | % of Households |
|---------|---------|-----------------|
| 0 | Steady Low Consumers | 44% |
| 1 | High Daytime (electric heating) | 12% |
| 2 | Minimal / Vacation homes | 20% |
| 3 | Night/Evening (EV/night heaters) | 24% |

## 🏗️ Project Structure

```
DataScienceLab-1/
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── .gitignore
│
├── src/                      # Main pipeline scripts
│   ├── 01_merge_data.py      # ETL: Merge all data sources
│   ├── 02_run_clustering.py  # K-Means household clustering
│   ├── 03_train_consumption_model.py   # XGBoost forecasting
│   ├── 04_train_cluster_forecast.py    # Per-cluster forecasting
│   └── utils/                # Helper modules
│       ├── clustering.py
│       ├── consumption_forecasting.py
│       ├── cluster_forecasting.py
│       └── eda_analysis.py
│
├── dashboard/                # Flask web dashboard
│   ├── flask_app.py          # Main application
│   ├── Dockerfile            # Docker container config
│   ├── docker-compose.yaml   # Docker compose
│   └── templates/            # HTML templates
│
├── data/
│   ├── raw/                  # Original meter JSON files (gitignored)
│   │   └── energy-data/      # 1000 meter files
│   ├── external/             # Reference data
│   │   ├── meter_info.csv
│   │   ├── sk_holidays_2016.csv
│   │   ├── sk_income.csv
│   │   ├── sk_population.csv
│   │   └── sk_zip_coordinates_clean.csv
│   └── processed/            # Generated outputs (gitignored)
│
├── outputs/
│   ├── reports/              # Analysis findings
│   ├── plots/                # Visualizations
│   │   ├── eda/
│   │   ├── clustering/
│   │   └── forecasting/
│   ├── models/               # Trained XGBoost models
│   └── results/              # Clustering assignments
│
└── notebooks/                # Jupyter notebooks
```

## 🚀 Getting Started

### Prerequisites

- Python 3.9+
- ~4GB RAM for processing

### Installation

```bash
# Clone repository
git clone https://github.com/AndiCodesss/DataScienceLab.git
cd DataScienceLab

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Running the Pipeline

Execute scripts in order from the project root:

```bash
# 1. Merge all data sources (requires raw data)
python src/01_merge_data.py

# 2. Run household clustering
python src/02_run_clustering.py

# 3. Train consumption forecasting model
python src/03_train_consumption_model.py --sample 100 --clusters outputs/results/meter_clusters.csv

# 4. Train per-cluster forecasting models
python src/04_train_cluster_forecast.py --horizon 24
```

## 📈 Methodology

### Data Pipeline

1. **Raw Data**: 1,000 meter JSON files with 15-minute interval consumption data
2. **Aggregation**: Converted to hourly intervals (~9.5M records)
3. **Enrichment**: Merged with:
   - Weather data (Meteostat API)
   - Geographic coordinates
   - Population by NUTS-3 region
   - Income distribution

### Clustering Approach

We use K-Means clustering on behavioral features:

- `avg_daily_consumption`: Overall magnitude
- `winter_summer_ratio`: Seasonality/heating indicator
- `night_ratio`: Time-of-use pattern
- `cv`: Consumption variability

### Forecasting Model

XGBoost Regressor with key features:
- **Lag features**: lag_1, lag_24, lag_168 (hourly, daily, weekly)
- **Temporal**: hour, day_of_week, month
- **Categorical**: cluster ID

Top feature importance:
1. `lag_168` (57.2%) - Same hour last week
2. `lag_24` (17.6%) - Same hour yesterday
3. `lag_1` (6.3%) - Previous hour
4. `cluster` (1.9%) - Household segment

## 📝 Key Findings

1. **Temporal Patterns Dominate**: Weekly and daily cycles explain most variance
2. **Electricity ≠ Heating**: Weak temperature correlation suggests gas/district heating
3. **Clustering Adds Value**: ~16% improvement in forecasting accuracy
4. **Weekend Effect**: 17% lower consumption on weekends

## 📄 Reports

Detailed analysis available in `outputs/reports/`:
- `clustering.txt` - Clustering methodology and results
- `eda_interpretation_report.txt` - Exploratory data analysis
- `cluster_forecasting.txt` - Per-cluster model performance


## 📜 License

This project was developed for the KPMG Data Science Challenge.
