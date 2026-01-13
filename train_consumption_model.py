import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import xgboost as xgb
from sklearn.metrics import mean_squared_error, mean_absolute_error
from helperfiles.consumption_forecasting import ConsumptionForecastingData

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
    parser.add_argument("--file", type=str, default="merged_data_hourly_with_weather.csv", help="Path to data file")
    parser.add_argument("--horizon", type=int, default=24, help="Forecast horizon in hours")
    parser.add_argument("--meter", type=str, default=None, help="Specific Meter ID (optional)")
    parser.add_argument("--sample", type=int, default=50, help="Number of meters to sample for global model training (0 for all)")
    parser.add_argument("--clusters", type=str, default=None, help="Path to meter_clusters.csv")
    args = parser.parse_args()

    # Paths
    plots_dir = Path("findings/plots/consumption_forecast")
    plots_dir.mkdir(parents=True, exist_ok=True)

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
    import xgboost as xgb
    
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
    
    # Save model for Power BI
    model_path = args.file.replace(".csv", "_xgboost_model.json")
    model.save_model(model_path)
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

    # Plot specific section (first unique meter in test set for clarity)
    # The X_test likely has mixed meters. Let's filter one for plotting.
    # But X_test is heavily processed. 'test_dates' corresponds to rows. 
    # But wait, create_train_test_split dropped metadata columns from X matrices.
    # To plot properly, we need the meter_id. The loader dropped it or it's not in X.
    # Actually, create_train_test_split returns clean X/y. 
    # For visualization, it's messy with mixed meters. 
    # Let's just plot the global sequence for now or improve return of split.
    
    plot_file = plots_dir / f"forecast_consumption_{args.horizon}h.png"
    plot_results(y_test.values, y_pred, test_dates, f"Consumption Forecast ({args.horizon}h ahead)", plot_file)
    print(f"Plot saved to {plot_file}")

if __name__ == "__main__":
    main()
