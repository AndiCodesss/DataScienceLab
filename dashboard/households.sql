
DROP TABLE IF EXISTS meter_readings;
CREATE TABLE meter_readings (
  meter_id                     INT NOT NULL,
  "timestamp"                  TIMESTAMP NOT NULL,
  "date"                       DATE NOT NULL,
  interval_index               INT NOT NULL,

  consumption                  DOUBLE PRECISION,
  laggingReactivePower         DOUBLE PRECISION,
  leadingReactivePower         DOUBLE PRECISION,

  temperature                  DOUBLE PRECISION,
  dew_point                    DOUBLE PRECISION,
  relative_humidity            DOUBLE PRECISION,
  precipitation                DOUBLE PRECISION,
  snow_depth                   DOUBLE PRECISION,
  wind_direction               DOUBLE PRECISION,
  wind_speed                   DOUBLE PRECISION,
  wind_gust                    DOUBLE PRECISION,
  pressure                     DOUBLE PRECISION,
  sunshine                     DOUBLE PRECISION,

  weather_condition            TEXT,

  "weekday"                      VARCHAR(10),
  "hour"                         SMALLINT,
  "month"                        SMALLINT,
  day_of_month                 SMALLINT,
  is_weekend                   BOOLEAN,
  is_holiday                   BOOLEAN,

  zip_code                     VARCHAR(10),
  sk_region_code               VARCHAR(10),
  region_name                  TEXT,
  region_city                  TEXT,

  latitude                     DOUBLE PRECISION,
  longitude                    DOUBLE PRECISION,

  "Population"                 DOUBLE PRECISION,

  households_income_bracket_1  DOUBLE PRECISION,
  households_income_bracket_2  DOUBLE PRECISION,
  households_income_bracket_3  DOUBLE PRECISION,
  households_income_bracket_4  DOUBLE PRECISION,
  households_income_bracket_5  DOUBLE PRECISION,
  households_income_bracket_6  DOUBLE PRECISION,
  households_income_bracket_7  DOUBLE PRECISION,
  households_income_bracket_8  DOUBLE PRECISION,
  households_income_bracket_9  DOUBLE PRECISION,
  households_income_bracket_10 DOUBLE PRECISION,
  households_income_bracket_11 DOUBLE PRECISION,
  households_total             DOUBLE PRECISION,

  PRIMARY KEY (meter_id, "timestamp")
);

-- Table for triggers from Metabase
DROP TABLE IF EXISTS forecasting_trigger;
CREATE TABLE forecasting_trigger (
    id SERIAL PRIMARY KEY,
    region VARCHAR(50), 
    "start_date" TIMESTAMP,
    hours_ahead INT DEFAULT 24,
    "status" VARCHAR(20) DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    "message" TEXT
);

-- Cloned table for forecasts
DROP TABLE IF EXISTS forecasts_meter_readings;
CREATE TABLE forecasts_meter_readings (
 meter_id                     INT NOT NULL,
  "timestamp"                  TIMESTAMP NOT NULL,
  "date"                       DATE NOT NULL,
  interval_index               INT NOT NULL,

  consumption                  DOUBLE PRECISION,
  laggingReactivePower         DOUBLE PRECISION,
  leadingReactivePower         DOUBLE PRECISION,

  temperature                  DOUBLE PRECISION,
  dew_point                    DOUBLE PRECISION,
  relative_humidity            DOUBLE PRECISION,
  precipitation                DOUBLE PRECISION,
  snow_depth                   DOUBLE PRECISION,
  wind_direction               DOUBLE PRECISION,
  wind_speed                   DOUBLE PRECISION,
  wind_gust                    DOUBLE PRECISION,
  pressure                     DOUBLE PRECISION,
  sunshine                     DOUBLE PRECISION,

  weather_condition            TEXT,

  "weekday"                      VARCHAR(10),
  "hour"                         SMALLINT,
  "month"                        SMALLINT,
  day_of_month                 SMALLINT,
  is_weekend                   BOOLEAN,
  is_holiday                   BOOLEAN,

  zip_code                     VARCHAR(10),
  sk_region_code               VARCHAR(10),
  region_name                  TEXT,
  region_city                  TEXT,

  latitude                     DOUBLE PRECISION,
  longitude                    DOUBLE PRECISION,

  "Population"                 DOUBLE PRECISION,

  households_income_bracket_1  DOUBLE PRECISION,
  households_income_bracket_2  DOUBLE PRECISION,
  households_income_bracket_3  DOUBLE PRECISION,
  households_income_bracket_4  DOUBLE PRECISION,
  households_income_bracket_5  DOUBLE PRECISION,
  households_income_bracket_6  DOUBLE PRECISION,
  households_income_bracket_7  DOUBLE PRECISION,
  households_income_bracket_8  DOUBLE PRECISION,
  households_income_bracket_9  DOUBLE PRECISION,
  households_income_bracket_10 DOUBLE PRECISION,
  households_income_bracket_11 DOUBLE PRECISION,
  households_total             DOUBLE PRECISION,

  PRIMARY KEY (meter_id, "timestamp")
);