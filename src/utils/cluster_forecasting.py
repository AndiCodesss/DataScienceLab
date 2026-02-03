import pandas as pd
import numpy as np
from pathlib import Path
from typing import Tuple, Optional, Dict

class ClusterForecastingData:
    def __init__(self, data_path: str, cluster_path: str):
        self.data_path = Path(data_path)
        self.cluster_path = Path(cluster_path)
        self.df = None

    def load_cluster_data(self, cluster_id: int, sample_n: int = 0) -> pd.DataFrame:
        """
        Loads consumption data for all meters (or a sample) in a specific cluster.
        Returns a DataFrame indexed by [meter_id, timestamp].
        """
        print(f"Loading data for Cluster {cluster_id} from {self.data_path}...")
        
        # 1. Load Clusters to find relevant meters
        cluster_df = pd.read_csv(self.cluster_path, dtype={'meter_id': 'str', 'cluster': 'int8'})
        target_meters = cluster_df[cluster_df['cluster'] == cluster_id]['meter_id'].unique()
        
        print(f"Cluster {cluster_id} has {len(target_meters)} meters.")
        
        # Sampling logic
        if sample_n > 0 and len(target_meters) > sample_n:
            print(f"Sampling {sample_n} random meters for training...")
            target_meters = np.random.choice(target_meters, sample_n, replace=False)

        # 2. Load Consumption Data (Optimized: Filter while loading if possible, but here we filter after)
        # We need to load all relevant columns
        usecols = ['timestamp', 'meter_id', 'consumption', 'temperature', 
                   'hour', 'date', 'is_holiday', 'is_weekend', 'day_of_week']
        
        dtype_dict = {
            'meter_id': 'str',
            'consumption': 'float32',
            'temperature': 'float32',
            'hour': 'int8',
            'is_holiday': 'int8',
            'is_weekend': 'int8'
        }
        
        # Load and filter
        df = pd.read_csv(
            self.data_path, 
            usecols=lambda c: c in usecols, 
            parse_dates=['timestamp'], 
            dtype=dtype_dict
        )
        
        # Filter for our cluster's meters
        df = df[df['meter_id'].isin(target_meters)]
        
        # Sort for time series ops
        df = df.sort_values(['meter_id', 'timestamp'])
        
        self.df = df
        return self.df

    def create_features(self, df: pd.DataFrame, target_col: str = 'consumption', lags: int = 24) -> pd.DataFrame:
        """
        Generates time-series features (lags, cyclic time) for the panel data.
        """
        print("Generating features...")
        df = df.copy()
        
        # 1. Cyclic Time Features
        df['hour_sin'] = np.sin(2 * np.pi * df['timestamp'].dt.hour / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['timestamp'].dt.hour / 24)
        
        day_of_year = df['timestamp'].dt.dayofyear
        df['day_sin'] = np.sin(2 * np.pi * day_of_year / 365.25)
        df['day_cos'] = np.cos(2 * np.pi * day_of_year / 365.25)
        
        # 2. Lag Features (Respecting Meter boundaries in the Panel)
        # Group by meter_id so lags are specific to the household
        grouped_target = df.groupby('meter_id')[target_col]
        
        # Short-term lags
        for lag in range(1, lags + 1):
            df[f'lag_{lag}'] = grouped_target.shift(lag)
            
        # Seasonal lags (Weekly)
        df['lag_168'] = grouped_target.shift(168)
        
        return df

    def create_train_test_split(self, df: pd.DataFrame, target_col: str, horizon: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.Series, pd.DataFrame]:
        """
        Prepares (X, y) for the cluster panel.
        Returns: X_train, y_train, X_test, y_test, test_df (for aggregation)
        """
        
        # Target: t+horizon
        # Group by meter to shift correctly
        df[f'target_{horizon}h'] = df.groupby('meter_id')[target_col].shift(-horizon)
        
        # Drop NaNs created by lags and target shift
        df = df.dropna()
        
        # Split by time (Global cut for all meters)
        dates = df['timestamp'].sort_values().unique()
        split_idx = int(len(dates) * 0.8)
        split_date = dates[split_idx]
        
        train = df[df['timestamp'] < split_date]
        test = df[df['timestamp'] >= split_date]
        
        # Features
        exclude = ['timestamp', 'meter_id', 'cluster', 'date', target_col, f'target_{horizon}h']
        features = [c for c in df.columns if c not in exclude]
        
        X_train = train[features]
        y_train = train[f'target_{horizon}h']
        X_test = test[features]
        y_test = test[f'target_{horizon}h']
        
        return X_train, y_train, X_test, y_test, test
