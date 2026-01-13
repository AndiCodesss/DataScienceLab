import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Tuple, Dict, Optional

class HouseholdClustering:
    def __init__(self, data_path: Path):
        self.data_path = data_path
        self.df = None
        self.features_df = None
        self.scaled_features = None
        self.kmeans = None
        self.labels = None

    def load_data(self):
        """Loads data with optimized types for memory efficiency."""
        print(f"Loading data from {self.data_path}...")
        dtypes = {
            'meter_id': 'category',
            'consumption': 'float32',
            'temperature': 'float32',
            'hour': 'int8',
            'month': 'int8'
        }
        # Only load necessary columns
        usecols = ['meter_id', 'timestamp', 'consumption', 'temperature', 'hour', 'month']
        self.df = pd.read_csv(
            self.data_path, 
            usecols=usecols, 
            dtype=dtypes, 
            parse_dates=['timestamp']
        )
        print(f"Loaded {len(self.df):,} rows.")

    def extract_features(self) -> pd.DataFrame:
        """Extracts behavioral features for each meter."""
        print("Extracting behavioral features per household...")
        
        if self.df is None:
            self.load_data()

        # 1. Pivot for simpler calculations (Meter x Time) if allowed by memory, 
        # but aggregation is safer. 

        # --- Aggregations ---
        # Group by meter
        print("   Calculating basic stats...")
        grouped = self.df.groupby('meter_id', observed=True)
        
        # Magnitude
        features = grouped['consumption'].agg([
            ('avg_daily_consumption', lambda x: x.mean() * 24),
            ('peak_load', 'max'),
            ('std_consumption', 'std')
        ])
        features['cv'] = features['std_consumption'] / (features['avg_daily_consumption'] / 24)

        # Seasonality
        print("   Calculating seasonal ratios...")
        # Winter: Dec, Jan, Feb; Summer: Jun, Jul, Aug
        winter_mask = self.df['month'].isin([12, 1, 2])
        summer_mask = self.df['month'].isin([6, 7, 8])
        
        winter_avg = self.df[winter_mask].groupby('meter_id', observed=True)['consumption'].mean()
        summer_avg = self.df[summer_mask].groupby('meter_id', observed=True)['consumption'].mean()
        
        # Avoid division by zero
        summer_avg = summer_avg.replace(0, 0.001)
        features['winter_summer_ratio'] = winter_avg / summer_avg

        # Timing (Time of Day Ratios)
        print("   Calculating time-of-day ratios...")
        # Night: 22-06, Morning: 06-10, Day: 10-18, Evening: 18-22
        self.df['tod'] = pd.cut(self.df['hour'], 
                               bins=[-1, 5, 9, 17, 21, 24], 
                               labels=['Night', 'Morning', 'Day', 'Evening', 'Night_late'],
                               ordered=False)
        # Fix Night_late label to Night
        self.df['tod'] = self.df['tod'].replace({'Night_late': 'Night'})
        
        tod_counts = self.df.groupby(['meter_id', 'tod'], observed=True)['consumption'].sum().unstack()
        total_consumption = tod_counts.sum(axis=1)
        
        for col in tod_counts.columns:
            features[f'{col.lower()}_ratio'] = tod_counts[col] / total_consumption

        # Temperature Correlation (if temp exists)
        print("   Calculating temperature sensitivity...")
        # Need to align properly. Using non-optimized corr for speed on loop or apply
        # Efficient way: Covariance / (std_x * std_y)
        # For now, let's just use a simple correlation on daily means to reduce data size
        daily = self.df.groupby(['meter_id', self.df['timestamp'].dt.date], observed=True)[['consumption', 'temperature']].mean()
        # Group by meter_id again on the daily data
        corrs = daily.groupby('meter_id').corr().iloc[0::2, 1].droplevel(1)
        # Sometimes corr is NaN if constant consumption
        features['temp_corr'] = corrs

        # Fill NaNs
        features = features.fillna(0)
        
        # Clean infinite values
        features = features.replace([np.inf, -np.inf], 0)
        
        self.features_df = features
        return features

    def perform_clustering(self, k: int = 5):
        """Scales features and runs K-Means."""
        print(f"Running K-Means with k={k}...")
        scaler = StandardScaler()
        self.scaled_features = scaler.fit_transform(self.features_df)
        
        self.kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        self.labels = self.kmeans.fit_predict(self.scaled_features)
        
        self.features_df['cluster'] = self.labels
        return self.labels

    def find_optimal_k(self, max_k: int = 10, plots_dir: Path = Path("findings/plots")):
        """Iterates through K=2..max_k to find optimal K using Elbow & Silhouette."""
        print("Determining optimal K...")
        scaler = StandardScaler()
        X = scaler.fit_transform(self.features_df)
        
        inertias = []
        silhouettes = []
        
        K_range = range(2, max_k + 1)
        for k in K_range:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = kmeans.fit_predict(X)
            inertias.append(kmeans.inertia_)
            silhouettes.append(silhouette_score(X, labels))
            print(f"   k={k}: Silhouette={silhouettes[-1]:.3f}")
            
        # Plot
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5))
        
        ax1.plot(K_range, inertias, 'bo-')
        ax1.set_title('Elbow Method (Inertia)')
        ax1.set_xlabel('k')
        
        ax2.plot(K_range, silhouettes, 'ro-')
        ax2.set_title('Silhouette Score')
        ax2.set_xlabel('k')
        
        plt.savefig(plots_dir / "clustering_optimization.png")
        plt.close()

    def plot_clusters(self, plots_dir: Path):
        """Generates visualizations for the clusters."""
        if 'cluster' not in self.features_df.columns:
            return

        # 1. Feature Distribution per Cluster (Boxplots)
        cols_to_plot = [c for c in self.features_df.columns if c != 'cluster']
        n_cols = 3
        n_rows = (len(cols_to_plot) + n_cols - 1) // n_cols
        
        fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 5*n_rows))
        axes = axes.flatten()
        
        for i, col in enumerate(cols_to_plot):
            sns.boxplot(x='cluster', y=col, data=self.features_df, ax=axes[i])
            axes[i].set_title(col)
            
        plt.tight_layout()
        plt.savefig(plots_dir / "cluster_features_boxplot.png")
        plt.close()
        
    def save_results(self, output_path: Path):
        self.features_df.to_csv(output_path)
        print(f"Results saved to {output_path}")
