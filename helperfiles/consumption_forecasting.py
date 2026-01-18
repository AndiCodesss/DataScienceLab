import pandas as pd
import numpy as np
from pathlib import Path
from typing import Tuple, List, Optional, Dict

class ConsumptionForecastingData:
    def __init__(self, data_path: str):
        self.data_path = Path(data_path)
        self.df = None

    def load_and_preprocess(self, target_col: str = 'consumption', 
                           zip_code: Optional[str] = None,
                           meter_id: Optional[str] = None,
                           sample_meters: int = 0,
                           cluster_file: Optional[str] = None) -> pd.DataFrame:
        """
        Loads consumption data and prepares it for forecasting.
        
        Args:
            target_col: Column to forecast (default: 'consumption')
            zip_code: Filter by ZIP (optional)
            meter_id: Filter by specific meter (optional)
            sample_meters: If > 0, randomly sample this many meters to reduce memory usage.
            cluster_file: Path to CSV containing meter_id to cluster mappings.
        """
        print(f"Loading data from {self.data_path}...")
        
        # Load necessary columns
        usecols = ['timestamp', 'meter_id', 'zip_code', 'date', 'hour', 'is_holiday', 'is_weekend', 'region_name', target_col]
        
        dtype_dict = {
            'meter_id': 'str',
            'zip_code': 'str',
            'region_name': 'category',
            'season': 'category',
            target_col: 'float32'
        }

        # Optimized load
        df = pd.read_csv(self.data_path, usecols=lambda c: c in usecols, parse_dates=['timestamp'], dtype=dtype_dict)
        
        # Merge Clusters if provided
        if cluster_file:
            print(f"Loading clusters from {cluster_file}...")
            cluster_df = pd.read_csv(cluster_file)
            # Ensure meter_id is string and index/column match
            if 'cluster' in cluster_df.columns:
                # Assuming simple CSV with meter_id (or index) and cluster
                # If meter_id is index, reset it.
                 # Check if meter_id is in columns, else assume index
                if 'meter_id' not in cluster_df.columns and cluster_df.index.name == 'meter_id':
                     cluster_df = cluster_df.reset_index()
                
                # Ensure str type
                if 'meter_id' in cluster_df.columns:
                    cluster_df['meter_id'] = cluster_df['meter_id'].astype(str)
                    
                    df = df.merge(cluster_df[['meter_id', 'cluster']], on='meter_id', how='left')
                    print("Merged cluster labels.")
                    
                    # Fill missing clusters with -1 or a specific category
                    df['cluster'] = df['cluster'].fillna(-1).astype('int8').astype('category')
                else:
                    print("Warning: 'meter_id' column not found in cluster file. Skipping merge.")
            else:
                 print("Warning: 'cluster' column not found in cluster file. Skipping merge.")
        
        # Filter logic
        if zip_code:
            df = df[df['zip_code'] == str(zip_code)]
        
        if meter_id:
            df = df[df['meter_id'] == str(meter_id)]
            
        if sample_meters > 0 and not meter_id:
            print(f"Sampling {sample_meters} meters...")
            unique_meters = df['meter_id'].unique()
            if len(unique_meters) > sample_meters:
                selected_meters = np.random.choice(unique_meters, sample_meters, replace=False)
                df = df[df['meter_id'].isin(selected_meters)]
        
        # Sort for time series features
        print("Sorting data...")
        df = df.sort_values(['meter_id', 'timestamp'])
        
        # Handle NA in target (drop rows where target is missing, can't train/test on them)
        df = df.dropna(subset=[target_col])
        
        # Feature Engineering
        print("Generating features...")
        df = self._add_cyclic_features(df)
        df = self._add_lag_features(df, target_col, lags=24) # 24 hourly lags
        
        # Add day-of-week
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        
        self.df = df
        return df

    def _add_cyclic_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adds sine/cosine transforms for hour and day-of-year."""
        df['hour_sin'] = np.sin(2 * np.pi * df['timestamp'].dt.hour / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['timestamp'].dt.hour / 24)
        
        day_of_year = df['timestamp'].dt.dayofyear
        df['day_sin'] = np.sin(2 * np.pi * day_of_year / 365.25)
        df['day_cos'] = np.cos(2 * np.pi * day_of_year / 365.25)
        return df

    def _add_lag_features(self, df: pd.DataFrame, target: str, lags: int) -> pd.DataFrame:
        """Adds lag features for the target variable."""
        # Group by meter_id to ensure lags don't cross meters
        grouped = df.groupby('meter_id')[target]
        
        # Short-term lags (1h, 2h ... 24h)
        for lag in range(1, lags + 1):
            df[f'lag_{lag}'] = grouped.shift(lag)
            
        # Longer term lags
        df['lag_168'] = grouped.shift(168) # Same hour last week
        
        return df

    def create_train_test_split(self, df: pd.DataFrame, target: str, horizon: int) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series, pd.Series]:
        """
        Creates train/test split respecting time.
        """
        df = df.copy()
        
        # Target: t+horizon
        df[f'target_{horizon}h'] = df.groupby('meter_id')[target].shift(-horizon)
        
        # Drop NaNs from feature/target creation
        df = df.dropna()
        
        splits = []
        # Split by time (global split point)
        # Using a global split ensures no future leakage across all meters
        dates = df['timestamp'].sort_values().unique()
        split_idx = int(len(dates) * 0.8)
        split_date = dates[split_idx]
        
        print(f"Splitting data at {split_date}...")
        
        train = df[df['timestamp'] < split_date]
        test = df[df['timestamp'] >= split_date]
        
        # Define features (Drop metadata and targets)
        exclude_cols = ['timestamp', 'meter_id', 'zip_code', 'date', 'hour', target, f'target_{horizon}h']
        feature_cols = [c for c in df.columns if c not in exclude_cols]
        # Include current consumption (lag_0) if available? 
        # Usually lags start at 1. Current value is technically lag_0 but usually we predict t+1 given t. 
        # If we predict t+h given t, we know X_t. Yes.
        
        print(f"Features: {feature_cols}")
        
        X_train = train[feature_cols]
        y_train = train[f'target_{horizon}h']
        X_test = test[feature_cols]
        y_test = test[f'target_{horizon}h']
        
        return X_train, y_train, X_test, y_test, test['timestamp']
