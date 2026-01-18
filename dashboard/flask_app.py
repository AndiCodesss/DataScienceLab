
import os
import logging
import sys
import pandas as pd
import numpy as np
import xgboost as xgb
import threading
import time
from flask import Flask, request, jsonify, render_template
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from pathlib import Path
from datetime import datetime

# Initialize Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Configuration ---
# Database Config - Placeholders to be filled by user
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "dbapp")
DB_USER = os.getenv("DB_USER", "app")
DB_PASSWORD = os.getenv("DB_PASSWORD", "apppassword")

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# --- Path Configuration ---
BASE_DIR = Path("..")
DATA_FILE = BASE_DIR / "merged_data_hourly_with_weather.csv"
MODEL_FILE = Path("merged_data_hourly_with_weather_xgboost_model.json")

# Add helper directory to path for imports
sys.path.append(str(BASE_DIR))
try:
    from helperfiles.consumption_forecasting import ConsumptionForecastingData
except ImportError:
    logger.error("Could not import ConsumptionForecastingData. Ensure helperfiles directory exists.")

# --- Global State ---
state = {
    "model": None,
    "df": None,
    "meters": [],
    "regions": []
}

# --- Database Models ---

# 1. Trigger Table from Metabase
class ForecastingTrigger(db.Model):
    __tablename__ = 'forecasting_trigger'
    
    id = db.Column(db.Integer, primary_key=True)
    region = db.Column(db.String(50), nullable=True) # "London", "SK01", etc.
    start_date = db.Column(db.DateTime, nullable=True)
    hours_ahead = db.Column(db.Integer, default=24) # User decides horizon
    # Status: PENDING, PROCESSING, COMPLETED, FAILED
    status = db.Column(db.String(20), default='PENDING') 
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    processed_at = db.Column(db.DateTime, nullable=True)
    message = db.Column(db.Text, nullable=True)

# 2. Results Table (Cloned from households)
class ForecastsMeterReading(db.Model):
    __tablename__ = 'forecasts_meter_readings'

    meter_id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, primary_key=True)
    
    date = db.Column(db.Date, nullable=True) 
    interval_index = db.Column(db.Integer, nullable=True)
    
    consumption = db.Column(db.Float)
    
    # Metadata columns
    laggingReactivePower = db.Column(db.Float, nullable=True)
    leadingReactivePower = db.Column(db.Float, nullable=True)
    temperature = db.Column(db.Float, nullable=True)
    dew_point = db.Column(db.Float, nullable=True)
    relative_humidity = db.Column(db.Float, nullable=True)
    precipitation = db.Column(db.Float, nullable=True)
    snow_depth = db.Column(db.Float, nullable=True)
    wind_direction = db.Column(db.Float, nullable=True)
    wind_speed = db.Column(db.Float, nullable=True)
    wind_gust = db.Column(db.Float, nullable=True)
    pressure = db.Column(db.Float, nullable=True)
    sunshine = db.Column(db.Float, nullable=True)
    weather_condition = db.Column(db.Text, nullable=True)
    weekday = db.Column(db.String(10), nullable=True)
    hour = db.Column(db.Integer, nullable=True)
    month = db.Column(db.Integer, nullable=True)
    day_of_month = db.Column(db.Integer, nullable=True)
    is_weekend = db.Column(db.Boolean, nullable=True)
    is_holiday = db.Column(db.Boolean, nullable=True)
    zip_code = db.Column(db.String(10), nullable=True)
    sk_region_code = db.Column(db.String(10), nullable=True)
    region_name = db.Column(db.Text, nullable=True)
    region_city = db.Column(db.Text, nullable=True)
    latitude = db.Column(db.Float, nullable=True)
    longitude = db.Column(db.Float, nullable=True)
    Population = db.Column(db.Float, nullable=True)
    households_total = db.Column(db.Float, nullable=True)

# --- Helper Functions ---

def construct_features(meter_id: str, start_time: pd.Timestamp, history_df: pd.DataFrame, hours_ahead: int) -> pd.DataFrame:
    """Generate features for 'hours_ahead' hours starting from start_time."""
    
    # 1. Create Future Timestamps
    future_dates = [start_time + pd.Timedelta(hours=i) for i in range(1, hours_ahead + 1)]
    future_df = pd.DataFrame({'timestamp': future_dates})
    future_df['meter_id'] = meter_id
    
    # 2. Cyclic & Calendar Features
    future_df['hour_sin'] = np.sin(2 * np.pi * future_df['timestamp'].dt.hour / 24)
    future_df['hour_cos'] = np.cos(2 * np.pi * future_df['timestamp'].dt.hour / 24)
    day = future_df['timestamp'].dt.dayofyear
    future_df['day_sin'] = np.sin(2 * np.pi * day / 365.25)
    future_df['day_cos'] = np.cos(2 * np.pi * day / 365.25)
    future_df['day_of_week'] = future_df['timestamp'].dt.dayofweek
    
    future_df['is_weekend'] = (future_df['day_of_week'] >= 5).astype(int)
    future_df['is_holiday'] = 0 
    
    # 3. Lag Features
    try:
        subset = history_df.loc[meter_id] 
    except KeyError:
        return pd.DataFrame() 

    # For lags, we usually need recent history relative to the prediction time.
    # Use standard 24h lags + 1 week lag.
    lags = list(range(1, 25)) + [168]
    
    for i, row in future_df.iterrows():
        ts = row['timestamp']
        for lag in lags:
            lookup_ts = ts - pd.Timedelta(hours=lag)
            try:
                # Direct lookup in history
                val = subset.loc[lookup_ts]['consumption'] if lookup_ts in subset.index else np.nan
            except KeyError:
                val = np.nan
            future_df.at[i, f'lag_{lag}'] = val
            
    return future_df

def run_forecast_logic(meter_id: str, start_time: pd.Timestamp, hours_ahead: int = 24):
    """Core forecasting logic returning list of reading objects."""
    if not state['model'] or state['df'] is None:
        raise RuntimeError("Model or Data not loaded.")
        
    # Generate Features
    # Note: If hours_ahead is large (e.g. >24h), simple lag lookup using only history fails 
    # because lag_1 for T+2 depends on T+1 (which is unknown).
    # Recursive forecasting is needed for correct multi-step ahead if horizon > 1 and we rely on short lags.
    # BUT, our simpler model might just use "known at T" lags? 
    # The current implementation 'construct_features' looks up lags relative to the prediction timestamp.
    # If we predict T+2, lag_1 looks for T+1. If T+1 is in the future, it finds NaN (or nothing) in history.
    # For a robust solution, we should implement RECURSIVE forecasting:
    # Predict T+1 -> Store as history -> Predict T+2 using T+1 ...
    
    readings = []
    current_step_time = start_time
    
    # We iterate 1 hour at a time for 'hours_ahead' steps
    # Note: This is computationally expensive but correct for autoregressive models.
    # However, updating the global 'df' state for every step is tricky with concurrency.
    # Instead, we build a local history buffer.
    
    # Get meter history
    try:
        meter_history = state['df'].loc[meter_id].copy()
        # Reset index to just timestamp for easier lookup
        # meter_history is likely Series or DataFrame with timestamp index?
        # state['df'] has MultiIndex (meter_id, timestamp). .loc[meter_id] returns DF with timestamp index.
    except KeyError:
         raise RuntimeError(f"Meter {meter_id} not found in history.")

    # We need a local cache of predictions to use as lags
    predictions_cache = {} # timestamp -> value
    
    feature_cols = ['is_weekend', 'is_holiday', 'hour_sin', 'hour_cos', 'day_sin', 'day_cos'] + \
                   [f'lag_{i}' for i in range(1, 25)] + ['lag_168', 'day_of_week']
                   
    lags = list(range(1, 25)) + [168]

    for h in range(1, hours_ahead + 1):
        target_ts = start_time + pd.Timedelta(hours=h)
        
        # 1. Build features for this single timestamp
        # Calendar
        hour_sin = np.sin(2 * np.pi * target_ts.hour / 24)
        hour_cos = np.cos(2 * np.pi * target_ts.hour / 24)
        day_of_year = target_ts.dayofyear
        day_sin = np.sin(2 * np.pi * day_of_year / 365.25)
        day_cos = np.cos(2 * np.pi * day_of_year / 365.25)
        day_of_week = target_ts.dayofweek
        is_weekend = int(day_of_week >= 5)
        is_holiday = 0
        
        row_dict = {
            'hour_sin': hour_sin, 'hour_cos': hour_cos,
            'day_sin': day_sin, 'day_cos': day_cos,
            'day_of_week': day_of_week,
            'is_weekend': is_weekend, 'is_holiday': is_holiday
        }
        
        # Lags
        for lag in lags:
            lookup_ts = target_ts - pd.Timedelta(hours=lag)
            
            # Key Logic: Check prediction cache first (recursive), then history
            if lookup_ts in predictions_cache:
                val = predictions_cache[lookup_ts]
            elif lookup_ts in meter_history.index:
                val = meter_history.loc[lookup_ts]['consumption']
            else:
                val = np.nan # Missing data
            
            row_dict[f'lag_{lag}'] = val
            
        # Create DF for prediction
        X_step = pd.DataFrame([row_dict])
        
        # Predict
        pred_val = float(state["model"].predict(X_step[feature_cols])[0])
        
        # Store in cache for next steps
        predictions_cache[target_ts] = pred_val
        
        # Add to results
        readings.append(ForecastsMeterReading(
            meter_id=int(meter_id),
            timestamp=target_ts,
            date=target_ts.date(),
            consumption=pred_val,
            hour=target_ts.hour,
            day_of_month=target_ts.day,
            month=target_ts.month,
            weekday=target_ts.day_name(),
            is_weekend=bool(is_weekend),
            is_holiday=bool(is_holiday)
        ))

    return readings

def process_trigger(trigger):
    """Process a single forecast request."""
    logger.info(f"Processing trigger ID {trigger.id} for Region: {trigger.region}")
    
    try:
        # Determine target meters
        subset_meters = state['meters']
        if trigger.region:
            df = state['df']
            meters_in_region = []
            unique_meter_ids = df.index.get_level_values('meter_id').unique()
            
            for m_id in unique_meter_ids:
                try:
                    m_df = df.loc[m_id]
                    if not m_df.empty:
                        r_name = m_df.iloc[0]['region_name']
                        if str(r_name).lower() == str(trigger.region).lower():
                            meters_in_region.append(m_id)
                except Exception:
                    continue
            
            if not meters_in_region:
                raise ValueError(f"No meters found in memory for region '{trigger.region}'.")
            
            subset_meters = meters_in_region
            
        logger.info(f"Forecasting for {len(subset_meters)} meters...")
        
        start_ts = pd.Timestamp(trigger.start_date) if trigger.start_date else pd.Timestamp("2017-01-01") 
        horizon = trigger.hours_ahead if trigger.hours_ahead else 24
        
        results = []
        for m_id in subset_meters:
            try:
                meter_readings = run_forecast_logic(str(m_id), start_ts, hours_ahead=horizon)
                results.extend(meter_readings)
            except Exception as e:
                logger.warning(f"Failed to forecast meter {m_id}: {e}")
                
        # Bulk Save
        if results:
            for r in results:
                db.session.merge(r)
            db.session.commit()
            
        trigger.status = 'COMPLETED'
        trigger.processed_at = datetime.utcnow()
        trigger.message = f"Success. Generated {len(results)} readings for {len(subset_meters)} meters."
        
    except Exception as e:
        logger.error(f"Trigger {trigger.id} failed: {e}")
        trigger.status = 'FAILED'
        trigger.processed_at = datetime.utcnow()
        trigger.message = str(e)
        
    db.session.commit()

def background_worker():
    """Polls for new triggers."""
    logger.info("Background worker started.")
    while True:
        try:
            with app.app_context():
                # Inspect triggers
                # We need to ensure tables exist first
                # (This creates tables if they don't exist, harmless valid check)
                db.create_all()
                
                pending = ForecastingTrigger.query.filter_by(status='PENDING').first()
                if pending:
                    pending.status = 'PROCESSING'
                    db.session.commit()
                    process_trigger(pending)
                else:
                    time.sleep(5) # Wait before next poll
        except Exception as e:
            logger.error(f"Worker loop error: {e}")
            time.sleep(5)

def load_resources():
    """Loads model and data into global state."""
    logger.info("Initializing resources...")
    
    if MODEL_FILE.exists():
        model = xgb.XGBRegressor()
        model.load_model(str(MODEL_FILE))
        state["model"] = model
        logger.info("XGBoost model loaded.")
    else:
        logger.warning(f"Model file not found at {MODEL_FILE}. Forecasts will fail.")

    if DATA_FILE.exists():
        logger.info("Loading data sample (this may take a moment)...")
        loader = ConsumptionForecastingData(str(DATA_FILE))
        # Load a sample for demo purposes.
        # Ensure region_name is loaded (we updated the helper).
        df = loader.load_and_preprocess(target_col='consumption', sample_meters=50)
        
        state["df"] = df.set_index(['meter_id', 'timestamp']).sort_index()
        state["meters"] = df.index.get_level_values('meter_id').unique().tolist()
        if 'region_name' in df.columns:
            state["regions"] = sorted(df['region_name'].dropna().astype(str).unique().tolist())
        else:
             state["regions"] = []
        logger.info(f"Loaded history for {len(state['meters'])} meters. Found regions: {state['regions']}")
    else:
        logger.warning(f"Data file not found at {DATA_FILE}. Forecasts will fail.")

# --- Routes ---

@app.route('/', methods=['GET'])
def index():
    """Serves the Forecasting UI."""
    return render_template('index.html', regions=state.get("regions", []))

@app.route('/trigger', methods=['POST'])
def trigger_forecast_endpoint():
    """Endpoint to trigger a forecast from the UI."""
    try:
        data = request.json
        region = data.get('region')
        start_date_str = data.get('start_date')
        hours_ahead = int(data.get('hours_ahead', 24))
        
        start_date = None
        if start_date_str:
            start_date = datetime.fromisoformat(start_date_str)
            
        # Create Trigger Object
        new_trigger = ForecastingTrigger(
            region=region,
            start_date=start_date,
            hours_ahead=hours_ahead,
            status='PENDING'
        )
        
        db.session.add(new_trigger)
        db.session.commit()
        
        return jsonify({
            "status": "queued",
            "task_id": new_trigger.id,
            "message": "Forecast task has been queued."
        })
        
    except Exception as e:
        logger.error(f"Error creating trigger: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        "status": "ok", 
        "worker": "active",
        "meters_loaded": len(state["meters"])
    })

# Keeps the direct endpoint for testing/manual use
@app.route('/forecast', methods=['POST'])
def manual_forecast():
    data = request.json
    meter_id = str(data.get('meter_id'))
    ts = data.get('timestamp')
    try:
        readings = run_forecast_logic(meter_id, pd.Timestamp(ts))
        for r in readings:
            db.session.merge(r)
        db.session.commit()
        return jsonify({"status": "ok", "count": len(readings)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- Main Entry ---
if __name__ == '__main__':
    load_resources()
    
    # Start Background Thread
    # Daemon thread dies when main program exits
    worker_thread = threading.Thread(target=background_worker, daemon=True)
    worker_thread.start()
    
    # Run server
    app.run(host='0.0.0.0', port=5000)
