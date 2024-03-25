drop table if exists tbl_batches;
create table if not exists tbl_batches (
    batch_id SERIAL PRIMARY KEY,
    batch_name VARCHAR(255) NOT NULL,
    batch_description VARCHAR(255) NOT NULL,
    status VARCHAR(10) default 'PROCESSING',
    inserted_at TIMESTAMP default CURRENT_TIMESTAMP,
    insert_user VARCHAR(50),
    updated_at TIMESTAMP,
    update_user VARCHAR(50)
);

drop table if exists dim_city;
-- Dimension Table for City
CREATE TABLE IF NOT EXISTS dim_city (
    city_id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    country VARCHAR(100) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    timezone INT NOT NULL,
    batch_id INT NOT NULL,
    status VARCHAR(10) DEFAULT 'ACTIVE'

);


CREATE UNIQUE INDEX idx_unique_active_city_country ON dim_city(name, country)
WHERE status = 'ACTIVE';

drop table if exists dim_date;
-- Dimension Table for Date with 3-hourly interval support
CREATE TABLE dim_date (
    date_key VARCHAR(10) PRIMARY KEY, --YYYYMMDDHH
    full_date TIMESTAMP unique,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT  NOT NULL,
    hour INT NOT NULL,
    weekday VARCHAR(50),
    status VARCHAR(10) default 'ACTIVE',
    inserted_at TIMESTAMP default CURRENT_TIMESTAMP,
    insert_user VARCHAR(50),
    updated_at  TIMESTAMP NULL,
    update_user VARCHAR(50) NULL
);


drop table if exists dim_weather_condition;
-- Dimension Table for Weather Conditions
CREATE TABLE dim_weather_condition (
    weather_condition_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description VARCHAR(255) NOT NULL,
    batch_id INT NOT NULL,
    status VARCHAR(10) default 'ACTIVE'
);

drop table if exists dim_weather_combination;
create TABLE dim_weather_combination (
    weather_combination_id BIGINT NOT NULL,
    weather_condition_id INT NOT NULL,
    batch_id INT NOT NULL,
    status VARCHAR(10) default 'ACTIVE'

);

CREATE UNIQUE INDEX idx_dim_weather_combination ON dim_weather_combination(weather_combination_id, weather_condition_id);


drop table if exists fact_forecast;
CREATE TABLE fact_forecast (
    forecast_id INT PRIMARY KEY,
    city_id INT NOT NULL,
    date_key VARCHAR(10) NOT NULL,
    population INT ,
    sunrise TIMESTAMP ,
    sunset TIMESTAMP ,
    temp FLOAT,
    feels_like FLOAT,
    temp_min FLOAT,
    temp_max FLOAT,
    pressure INT,
    sea_level INT,
    grnd_level INT,
    humidity INT,
    temp_kf FLOAT,
    wind_speed FLOAT,
    wind_deg INT,
    wind_gust FLOAT,
    clouds_all INT,
    visibility INT,
    rain_1h FLOAT,
    rain_2h FLOAT,
    rain_3h FLOAT,
    snow_1h FLOAT,
    snow_2h FLOAT,
    snow_3h FLOAT,
    weather_combination_id BIGINT,
    batch_id INT NOT NULL,
    status VARCHAR(10) default 'ACTIVE'
);

CREATE INDEX idx_city_date ON fact_forecast (city_id, date_key);

CREATE INDEX idx_status ON fact_forecast (status);

CREATE INDEX idx_batch_id ON fact_forecast (batch_id);

CREATE INDEX idx_weather_condition_ids ON fact_forecast (weather_combination_id);


drop table if exists fact_forecast_staging;
CREATE TABLE fact_forecast_staging (
    forecast_id SERIAL PRIMARY KEY,
    city_id INT NOT NULL,
    date_key VARCHAR(10) NOT NULL,
    population INT,
    sunrise TIMESTAMP ,
    sunset TIMESTAMP ,
    temp FLOAT,
    feels_like FLOAT,
    temp_min FLOAT,
    temp_max FLOAT,
    pressure INT,
    sea_level INT,
    grnd_level INT,
    humidity INT,
    temp_kf FLOAT,
    wind_speed FLOAT,
    wind_deg INT,
    wind_gust FLOAT,
    clouds_all INT,
    visibility INT,
    rain_1h FLOAT,
    rain_2h FLOAT,
    rain_3h FLOAT,
    snow_1h FLOAT,
    snow_2h FLOAT,
    snow_3h FLOAT,
    weather_combination_id BIGINT,
    batch_id INT NOT NULL,
    status VARCHAR(10) default 'ACTIVE'
);

CREATE INDEX idx_ffs_batch_id ON fact_forecast_staging (batch_id);