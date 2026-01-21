import pandas as pd
import numpy as np
from pathlib import Path
from typing import Tuple, Optional, Dict

class ClusterForecastingData:
    def __init__(self, data_path: str, cluster_path: str):
        self.data_path = Path(data_path)
        self.cluster_path = Path(cluster_path)
        self.df = None

    def load_aggregated_data(self, target_col: str = 'consumption') -> pd.DataFrame:
        """
        Loads consumption data, merges with clusters, and aggregates to cluster level.
        Returns a DataFrame indexed by [cluster, timestamp].
        """
        print(f"Loading data from {self.data_path}...")
        
        # 1. Load Consumption Data
        usecols = ['timestamp', 'meter_id', 'consumption', 'temperature', 'hour', 'date']
        dtype_dict = {
            'meter_id': 'str',
            'consumption': 'float32',
            'temperature': 'float32',
            'hour': 'int8'
        }
        
        df = pd.read_csv(
            self.data_path, 
            usecols=lambda c: c in usecols or c in ['is_holiday', 'is_weekend'], 
            parse_dates=['timestamp'], 
            dtype=dtype_dict
        )

        # 2. Load Clusters
        print(f"Loading clusters from {self.cluster_path}...")
        cluster_df = pd.read_csv(self.cluster_path, dtype={'meter_id': 'str', 'cluster': 'int8'})
        
        # 3. Merge
        # Inner join: We only care about meters that have a cluster assignment
        df = df.merge(cluster_df[['meter_id', 'cluster']], on='meter_id', how='inner')
        print(f"Merged data: {len(df):,} rows from {cluster_df['meter_id'].nunique()} meters.")

        # 4. Aggregate by Cluster and Time
        print("Aggregating by Cluster and Timestamp...")
        
        # Define aggregation dictionary
        agg_dict = {
            'consumption': 'mean',      # Representative profile (avg kWh per meter)
            'temperature': 'mean',      # Avg temp for the region/meters
            'is_holiday': 'max',        # 1 if holiday
            'is_weekend': 'max'         # 1 if weekend
        }
        
        # Handles potential missing cols (like is_holiday if not in source)
        available_aggs = {k: v for k, v in agg_dict.items() if k in df.columns}
        
        grouped = df.groupby(['cluster', 'timestamp'], observed=True).agg(available_aggs).reset_index()
        
        # Sort for time series ops
        grouped = grouped.sort_values(['cluster', 'timestamp'])
        
        self.df = grouped
        return self.df

    def create_features(self, df: pd.DataFrame, target_col: str = 'consumption', lags: int = 24) -> pd.DataFrame:
        """
        Generates time-series features (lags, cyclic time) for the aggregated data.
        """
        print("Generating features...")
        df = df.copy()
        
        # 1. Cyclic Time Features
        df['hour_sin'] = np.sin(2 * np.pi * df['timestamp'].dt.hour / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['timestamp'].dt.hour / 24)
        
        day_of_year = df['timestamp'].dt.dayofyear
        df['day_sin'] = np.sin(2 * np.pi * day_of_year / 365.25)
        df['day_cos'] = np.cos(2 * np.pi * day_of_year / 365.25)
        
        df['day_of_week'] = df['timestamp'].dt.dayofweek

        # 2. Lag Features (Respecting Cluster boundaries)
        # Group by cluster so lags don't bleed between clusters
        grouped_target = df.groupby('cluster')[target_col]
        
        # Short-term lags
        for lag in range(1, lags + 1):
            df[f'lag_{lag}'] = grouped_target.shift(lag)
            
        # Seasonal lags (Weekly)
        df['lag_168'] = grouped_target.shift(168)
        
        return df

    def create_train_test_split(self, df: pd.DataFrame, target_col: str, horizon: int, cluster_id: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series, pd.Series]:
        """
        Prepares (X, y) for a SPECIFIC cluster.
        """
        # Filter for specific cluster
        cluster_data = df[df['cluster'] == cluster_id].copy()
        
        # Target: t+horizon
        cluster_data[f'target_{horizon}h'] = cluster_data[target_col].shift(-horizon)
        
        # Drop NaNs
        cluster_data = cluster_data.dropna()
        
        # Split by time
        dates = cluster_data['timestamp'].sort_values().unique()
        split_idx = int(len(dates) * 0.8)
        split_date = dates[split_idx]
        
        train = cluster_data[cluster_data['timestamp'] < split_date]
        test = cluster_data[cluster_data['timestamp'] >= split_date]
        
        # Features
        exclude = ['timestamp', 'cluster', target_col, f'target_{horizon}h']
        features = [c for c in cluster_data.columns if c not in exclude]
        
        X_train = train[features]
        y_train = train[f'target_{horizon}h']
        X_test = test[features]
        y_test = test[f'target_{horizon}h']
        
        return X_train, y_train, X_test, y_test, test['timestamp']
