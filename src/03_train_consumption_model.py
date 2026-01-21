"""
03_train_consumption_model.py - Consumption Forecasting Model

This script trains an XGBoost model to forecast household energy consumption.
The model uses:
- Lag features (lag_1, lag_24, lag_168 for hourly, daily, weekly patterns)
- Temporal features (hour, day_of_week, month)
- Cluster ID (from K-Means clustering)

Output:
- outputs/models/xgboost_consumption_model.json
- outputs/plots/forecasting/forecast_consumption_*.png

Usage:
    python src/03_train_consumption_model.py
    python src/03_train_consumption_model.py --horizon 24 --sample 100
    python src/03_train_consumption_model.py --clusters outputs/results/meter_clusters.csv
"""

import argparse
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import xgboost as xgb
from sklearn.metrics import mean_squared_error, mean_absolute_error
from src.utils.consumption_forecasting import ConsumptionForecastingData


def plot_results(y_true, y_pred, dates, title, filename):
    plt.figure(figsize=(15, 7))
    # Plot last 1000 points
    limit = 1000
    if len(y_true) > limit:
        plt.plot(dates[-limit:], y_true[-limit:], label='Actual', alpha=0.7)
        plt.plot(dates[-limit:], y_pred[-limit:], label='Predicted', alpha=0.7, linestyle='--')
    else:
        plt.plot(dates, y_true, label='Actual', alpha=0.7)
        plt.plot(dates, y_pred, label='Predicted', alpha=0.7, linestyle='--')
        
    plt.title(title)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig(filename)
    plt.close()


def main():
    parser = argparse.ArgumentParser(description="Train Consumption Forecasting Model")
    parser.add_argument("--file", type=str, default="data/processed/merged_data_hourly_with_weather.csv", help="Path to data file")
    parser.add_argument("--horizon", type=int, default=24, help="Forecast horizon in hours")
    parser.add_argument("--meter", type=str, default=None, help="Specific Meter ID (optional)")
    parser.add_argument("--sample", type=int, default=50, help="Number of meters to sample for global model training (0 for all)")
    parser.add_argument("--clusters", type=str, default=None, help="Path to meter_clusters.csv")
    args = parser.parse_args()

    # Paths
    plots_dir = Path("outputs/plots/forecasting")
    plots_dir.mkdir(parents=True, exist_ok=True)
    models_dir = Path("outputs/models")
    models_dir.mkdir(parents=True, exist_ok=True)

    print(f"Training Consumption Model (Horizon: {args.horizon}h)...")

    # Load Data
    loader = ConsumptionForecastingData(args.file)
    df = loader.load_and_preprocess(
        target_col='consumption', 
        meter_id=args.meter,
        sample_meters=args.sample,
        cluster_file=args.clusters
    )
    
    # Split
    X_train, y_train, X_test, y_test, test_dates = loader.create_train_test_split(df, 'consumption', args.horizon)
    
    print(f"Train size: {X_train.shape}")
    print(f"Test size:  {X_test.shape}")
    
    # Train
    print("Training XGBRegressor...")
    
    model = xgb.XGBRegressor(
        n_estimators=1000, 
        learning_rate=0.05, 
        max_depth=6, 
        early_stopping_rounds=50,
        n_jobs=-1,
        random_state=42,
        enable_categorical=True
    )
    
    # Use test set for early stopping to prevent overfitting
    model.fit(
        X_train, y_train, 
        eval_set=[(X_train, y_train), (X_test, y_test)], 
        verbose=100
    )
    
    # Save model
    model_path = models_dir / "xgboost_consumption_model.json"
    model.save_model(str(model_path))
    print(f"Model saved to {model_path}")
    
    # Evaluate
    print("Predicting...")
    y_pred = model.predict(X_test)
    
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)
    
    print("\n" + "="*40)
    print(f"RESULTS ({args.horizon}h Horizon)")
    print("="*40)
    print(f"RMSE: {rmse:.4f}")
    print(f"MAE:  {mae:.4f}")
    
    # Feature Importance
    importances = model.feature_importances_
    indices = np.argsort(importances)[::-1]
    print("\nTop 5 Features:")
    for f in range(min(5, X_train.shape[1])):
        print(f"{f+1}. {X_train.columns[indices[f]]}: {importances[indices[f]]:.4f}")

    # Plot
    plot_file = plots_dir / f"forecast_consumption_{args.horizon}h.png"
    plot_results(y_test.values, y_pred, test_dates, f"Consumption Forecast ({args.horizon}h ahead)", plot_file)
    print(f"Plot saved to {plot_file}")


if __name__ == "__main__":
    main()
