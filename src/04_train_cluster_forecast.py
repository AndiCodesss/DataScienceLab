"""
04_train_cluster_forecast.py - Per-Cluster Forecasting Model

This script trains separate XGBoost models for each household cluster,
enabling more accurate aggregate forecasting at the cluster level.

This is useful for grid operators who need to forecast demand for 
different types of households (e.g., electric heating vs. steady consumers).

Output:
- outputs/plots/forecasting/cluster_*_forecast_*.png
- outputs/plots/forecasting/cluster_comparison_*.png
- outputs/reports/cluster_forecasting.txt

Usage:
    python src/04_train_cluster_forecast.py
    python src/04_train_cluster_forecast.py --horizon 24
"""

import argparse
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import xgboost as xgb
from sklearn.metrics import mean_squared_error, mean_absolute_error
from src.utils.cluster_forecasting import ClusterForecastingData


# Define cluster profile names for better visualization
CLUSTER_NAMES = {
    0: "Steady Low",
    1: "High Daytime",
    2: "Minimal",
    3: "Night/Evening"
}

# Color palette for consistent cluster colors
CLUSTER_COLORS = {
    0: "#2ecc71",  # Green
    1: "#e74c3c",  # Red
    2: "#9b59b6",  # Purple
    3: "#3498db"   # Blue
}


def plot_forecast(y_true, y_pred, dates, cluster_id, horizon, filename):
    """Plot actual vs predicted for a single cluster with enhanced styling."""
    fig, ax = plt.subplots(figsize=(14, 6))
    
    # Plot last 6 days (144 hours) for context
    limit = 144
    color = CLUSTER_COLORS.get(cluster_id, "#333333")
    name = CLUSTER_NAMES.get(cluster_id, f"Cluster {cluster_id}")
    
    if len(y_true) > limit:
        plot_dates = dates[-limit:]
        plot_true = y_true[-limit:]
        plot_pred = y_pred[-limit:]
    else:
        plot_dates = dates
        plot_true = y_true
        plot_pred = y_pred
    
    # Fill between for error visualization
    ax.fill_between(plot_dates, plot_true, plot_pred, alpha=0.2, color=color, label='Prediction Error')
    ax.plot(plot_dates, plot_true, label='Actual', color='#2c3e50', linewidth=2, alpha=0.9)
    ax.plot(plot_dates, plot_pred, label='Predicted', color=color, linewidth=2, linestyle='--', alpha=0.9)
    
    ax.set_title(f'{name} Cluster - {horizon}h Ahead Forecast', fontsize=14, fontweight='bold')
    ax.set_xlabel('Date', fontsize=11)
    ax.set_ylabel('Avg Consumption (kWh)', fontsize=11)
    ax.legend(loc='upper right', fontsize=10)
    ax.grid(True, alpha=0.3, linestyle='-')
    
    # Style improvements
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close()


def plot_performance_comparison(results_df, horizon, filename):
    """Bar chart comparing MAE and RMSE across clusters."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Add cluster names
    results_df['Cluster_Name'] = results_df['Cluster'].map(CLUSTER_NAMES)
    colors = [CLUSTER_COLORS.get(c, '#333') for c in results_df['Cluster']]
    
    # MAE comparison
    ax1 = axes[0]
    bars1 = ax1.bar(results_df['Cluster_Name'], results_df['MAE'], color=colors, edgecolor='white', linewidth=2)
    ax1.set_title('Mean Absolute Error by Cluster', fontsize=13, fontweight='bold')
    ax1.set_ylabel('MAE (kWh)', fontsize=11)
    ax1.set_xlabel('')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    
    # Add value labels
    for bar, val in zip(bars1, results_df['MAE']):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.02, 
                f'{val:.3f}', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # Error % comparison
    ax2 = axes[1]
    error_pct = (results_df['MAE'] / results_df['Mean_Cons']) * 100
    bars2 = ax2.bar(results_df['Cluster_Name'], error_pct, color=colors, edgecolor='white', linewidth=2)
    ax2.set_title('Relative Error by Cluster', fontsize=13, fontweight='bold')
    ax2.set_ylabel('Error (% of Mean)', fontsize=11)
    ax2.set_xlabel('')
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    
    # Add value labels
    for bar, val in zip(bars2, error_pct):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, 
                f'{val:.1f}%', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    plt.suptitle(f'Cluster Forecasting Performance ({horizon}h Horizon)', fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close()


def plot_consumption_profiles(df_agg, filename):
    """Heatmap showing average hourly consumption by cluster."""
    # Extract hour from timestamp
    df_plot = df_agg.copy()
    df_plot['hour'] = df_plot['timestamp'].dt.hour
    
    # Pivot to get hour x cluster matrix
    pivot = df_plot.groupby(['cluster', 'hour'])['consumption'].mean().unstack(level=0)
    pivot.columns = [CLUSTER_NAMES.get(c, f"Cluster {c}") for c in pivot.columns]
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    sns.heatmap(pivot, annot=True, fmt='.2f', cmap='YlOrRd', 
                linewidths=0.5, ax=ax, cbar_kws={'label': 'Avg kWh'})
    
    ax.set_title('Average Hourly Consumption by Cluster', fontsize=14, fontweight='bold')
    ax.set_xlabel('Cluster Type', fontsize=12)
    ax.set_ylabel('Hour of Day', fontsize=12)
    
    plt.tight_layout()
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close()


def plot_daily_profiles_overlay(df_agg, filename):
    """Line plot overlay showing consumption patterns for each cluster."""
    # Extract hour from timestamp
    df_plot = df_agg.copy()
    df_plot['hour'] = df_plot['timestamp'].dt.hour
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    for cluster_id in sorted(df_plot['cluster'].unique()):
        cluster_data = df_plot[df_plot['cluster'] == cluster_id]
        hourly_avg = cluster_data.groupby('hour')['consumption'].mean()
        
        name = CLUSTER_NAMES.get(cluster_id, f"Cluster {cluster_id}")
        color = CLUSTER_COLORS.get(cluster_id, '#333')
        
        ax.plot(hourly_avg.index, hourly_avg.values, 
                label=name, color=color, linewidth=3, marker='o', markersize=5)
    
    ax.set_title('Daily Consumption Profiles by Cluster', fontsize=14, fontweight='bold')
    ax.set_xlabel('Hour of Day', fontsize=12)
    ax.set_ylabel('Avg Consumption (kWh)', fontsize=12)
    ax.set_xticks(range(0, 24, 2))
    ax.legend(loc='upper right', fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    # Add time period annotations
    ax.axvspan(0, 6, alpha=0.1, color='blue', label='Night')
    ax.axvspan(17, 21, alpha=0.1, color='orange', label='Evening Peak')
    
    plt.tight_layout()
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close()


def plot_error_distribution(all_errors, filename):
    """Violin plot showing error distribution per cluster."""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Convert to DataFrame for seaborn
    error_df = pd.DataFrame(all_errors)
    error_df['Cluster_Name'] = error_df['cluster'].map(CLUSTER_NAMES)
    
    palette = [CLUSTER_COLORS.get(c, '#333') for c in sorted(error_df['cluster'].unique())]
    
    sns.violinplot(data=error_df, x='Cluster_Name', y='error', palette=palette, ax=ax)
    ax.axhline(y=0, color='red', linestyle='--', alpha=0.5, linewidth=2)
    
    ax.set_title('Prediction Error Distribution by Cluster', fontsize=14, fontweight='bold')
    ax.set_xlabel('')
    ax.set_ylabel('Prediction Error (kWh)', fontsize=12)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close()


def plot_summary_dashboard(results_df, horizon, filename):
    """Multi-panel summary dashboard."""
    fig = plt.figure(figsize=(16, 10))
    
    # Create grid
    gs = fig.add_gridspec(2, 3, hspace=0.3, wspace=0.3)
    
    results_df['Cluster_Name'] = results_df['Cluster'].map(CLUSTER_NAMES)
    colors = [CLUSTER_COLORS.get(c, '#333') for c in results_df['Cluster']]
    
    # Panel 1: Mean Consumption by Cluster (pie chart)
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.pie(results_df['Mean_Cons'], labels=results_df['Cluster_Name'], 
            colors=colors, autopct='%1.1f%%', startangle=90)
    ax1.set_title('Consumption Share by Cluster', fontsize=12, fontweight='bold')
    
    # Panel 2: MAE comparison (horizontal bar)
    ax2 = fig.add_subplot(gs[0, 1])
    y_pos = np.arange(len(results_df))
    ax2.barh(y_pos, results_df['MAE'], color=colors, edgecolor='white')
    ax2.set_yticks(y_pos)
    ax2.set_yticklabels(results_df['Cluster_Name'])
    ax2.set_xlabel('MAE (kWh)')
    ax2.set_title('Forecast Accuracy (MAE)', fontsize=12, fontweight='bold')
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    for i, v in enumerate(results_df['MAE']):
        ax2.text(v + 0.01, i, f'{v:.3f}', va='center', fontsize=10)
    
    # Panel 3: RMSE comparison
    ax3 = fig.add_subplot(gs[0, 2])
    ax3.barh(y_pos, results_df['RMSE'], color=colors, edgecolor='white')
    ax3.set_yticks(y_pos)
    ax3.set_yticklabels(results_df['Cluster_Name'])
    ax3.set_xlabel('RMSE (kWh)')
    ax3.set_title('Forecast Accuracy (RMSE)', fontsize=12, fontweight='bold')
    ax3.spines['top'].set_visible(False)
    ax3.spines['right'].set_visible(False)
    for i, v in enumerate(results_df['RMSE']):
        ax3.text(v + 0.01, i, f'{v:.3f}', va='center', fontsize=10)
    
    # Panel 4: Error percentage
    ax4 = fig.add_subplot(gs[1, :2])
    error_pct = (results_df['MAE'] / results_df['Mean_Cons']) * 100
    x_pos = np.arange(len(results_df))
    bars = ax4.bar(x_pos, error_pct, color=colors, edgecolor='white', linewidth=2)
    ax4.set_xticks(x_pos)
    ax4.set_xticklabels(results_df['Cluster_Name'])
    ax4.set_ylabel('Error (%)')
    ax4.set_title('Relative Forecast Error', fontsize=12, fontweight='bold')
    ax4.spines['top'].set_visible(False)
    ax4.spines['right'].set_visible(False)
    ax4.axhline(y=error_pct.mean(), color='red', linestyle='--', alpha=0.7, label=f'Avg: {error_pct.mean():.1f}%')
    ax4.legend()
    for bar, val in zip(bars, error_pct):
        ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3, 
                f'{val:.1f}%', ha='center', fontsize=11, fontweight='bold')
    
    # Panel 5: Summary stats text
    ax5 = fig.add_subplot(gs[1, 2])
    ax5.axis('off')
    summary_text = f"""
    Cluster Forecast Summary
    ─────────────────────────
    Horizon: {horizon} hours
    
    Best Performer:
    {results_df.loc[results_df['MAE'].idxmin(), 'Cluster_Name']}
    (MAE: {results_df['MAE'].min():.3f} kWh)
    
    Most Challenging:
    {results_df.loc[results_df['MAE'].idxmax(), 'Cluster_Name']}
    (MAE: {results_df['MAE'].max():.3f} kWh)
    
    Average MAE: {results_df['MAE'].mean():.3f} kWh
    Average Error: {error_pct.mean():.1f}%
    """
    ax5.text(0.1, 0.5, summary_text, transform=ax5.transAxes, fontsize=12,
             verticalalignment='center', fontfamily='monospace',
             bbox=dict(boxstyle='round', facecolor='#f8f9fa', edgecolor='#dee2e6'))
    
    plt.suptitle(f'Cluster Forecasting Dashboard', fontsize=16, fontweight='bold', y=0.98)
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close()


def main():
    parser = argparse.ArgumentParser(description="Train Cluster Forecasting Model")
    parser.add_argument("--data", type=str, default="data/processed/merged_data_hourly_with_weather.csv")
    parser.add_argument("--clusters", type=str, default="outputs/results/meter_clusters.csv")
    parser.add_argument("--horizon", type=int, default=24, help="Forecast horizon in hours (default 24)")
    args = parser.parse_args()

    # Setup
    plots_dir = Path("outputs/plots/forecasting")
    plots_dir.mkdir(parents=True, exist_ok=True)
    reports_dir = Path("outputs/reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("CLUSTER FORECASTING PIPELINE")
    print("=" * 60)
    
    loader = ClusterForecastingData(args.data, args.clusters)
    
    # 1. Load & Aggregate
    print("\n1. Loading and aggregating data...")
    df_agg = loader.load_aggregated_data()
    
    # 2. Generate consumption profile visualizations
    print("\n2. Generating consumption profile visualizations...")
    plot_daily_profiles_overlay(df_agg, plots_dir / "cluster_daily_profiles_overlay.png")
    plot_consumption_profiles(df_agg, plots_dir / "cluster_consumption_heatmap.png")
    print("   ✓ Daily profiles and heatmap saved")
    
    # 3. Features
    print("\n3. Creating features...")
    df_features = loader.create_features(df_agg, lags=24)
    
    unique_clusters = sorted(df_features['cluster'].unique())
    results = []
    all_errors = []

    print(f"\n4. Training models for {len(unique_clusters)} clusters...")
    
    # 4. Train per Cluster
    for cluster_id in unique_clusters:
        cluster_name = CLUSTER_NAMES.get(cluster_id, f"Cluster {cluster_id}")
        print(f"\n   --- {cluster_name} (ID: {cluster_id}) ---")
        
        X_train, y_train, X_test, y_test, dates = loader.create_train_test_split(
            df_features, 
            target_col='consumption', 
            horizon=args.horizon, 
            cluster_id=cluster_id
        )
        
        print(f"   Train samples: {len(X_train)}")
        
        model = xgb.XGBRegressor(
            n_estimators=1000,
            learning_rate=0.03,
            max_depth=5,
            early_stopping_rounds=50,
            n_jobs=-1,
            random_state=42
        )
        
        model.fit(
            X_train, y_train,
            eval_set=[(X_train, y_train), (X_test, y_test)],
            verbose=False
        )
        
        # Predict
        y_pred = model.predict(X_test)
        errors = y_pred - y_test.values
        
        # Store errors for violin plot
        for err in errors:
            all_errors.append({'cluster': cluster_id, 'error': err})
        
        # Metrics
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        mae = mean_absolute_error(y_test, y_pred)
        mean_cons = y_test.mean()
        
        print(f"   RMSE: {rmse:.4f} | MAE: {mae:.4f} | Error: {(mae/mean_cons)*100:.1f}%")
        
        results.append({
            'Cluster': cluster_id,
            'RMSE': rmse,
            'MAE': mae,
            'Mean_Cons': mean_cons
        })
        
        # Individual forecast plot
        plot_file = plots_dir / f"cluster_{cluster_id}_forecast_{args.horizon}h.png"
        plot_forecast(y_test.values, y_pred, dates, cluster_id, args.horizon, plot_file)

    # 5. Generate comparison visualizations
    print("\n5. Generating comparison visualizations...")
    res_df = pd.DataFrame(results)
    
    plot_performance_comparison(res_df, args.horizon, plots_dir / f"cluster_performance_comparison_{args.horizon}h.png")
    print("   ✓ Performance comparison chart saved")
    
    plot_error_distribution(all_errors, plots_dir / f"cluster_error_distribution_{args.horizon}h.png")
    print("   ✓ Error distribution plot saved")
    
    plot_summary_dashboard(res_df, args.horizon, plots_dir / f"cluster_forecast_dashboard_{args.horizon}h.png")
    print("   ✓ Summary dashboard saved")
    
    # 6. Summary
    print("\n" + "="*60)
    print("FINAL RESULTS")
    print("="*60)
    res_df['Cluster_Name'] = res_df['Cluster'].map(CLUSTER_NAMES)
    print(res_df[['Cluster_Name', 'RMSE', 'MAE', 'Mean_Cons']].to_string(index=False))
    
    # Save text summary
    report_file = reports_dir / "cluster_forecasting.txt"
    with open(report_file, "w") as f:
        f.write("Cluster Forecasting Results\n")
        f.write("=" * 40 + "\n\n")
        f.write(f"Forecast Horizon: {args.horizon} hours\n\n")
        f.write("Performance Metrics:\n")
        f.write("-" * 40 + "\n")
        f.write(res_df[['Cluster_Name', 'RMSE', 'MAE', 'Mean_Cons']].to_string(index=False))
        f.write("\n\n")
        f.write("Interpretation:\n")
        f.write("-" * 40 + "\n")
        f.write("- MAE (Mean Absolute Error): Average prediction deviation in kWh\n")
        f.write("- RMSE penalizes larger errors more heavily\n")
        f.write("- Lower values indicate better forecast accuracy\n")
        f.write(f"\nBest performing cluster: {res_df.loc[res_df['MAE'].idxmin(), 'Cluster_Name']}\n")
        f.write(f"Average MAE across clusters: {res_df['MAE'].mean():.4f} kWh\n")
    
    print(f"\n✓ Report saved to {report_file}")
    print(f"✓ All plots saved to {plots_dir}")


if __name__ == "__main__":
    main()
