## Data Loader - data_downloader

import requests
from mage_ai.data_preparation.shared.secrets import get_secret_value
import json
from jsonschema import validate
from jsonschema.exceptions import ValidationError

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs) -> str:
    """
    Template for loading data from API
    """
    logger = kwargs.get('logger')
    lat = kwargs.get('lat')
    lon = kwargs.get('lon')
    appid = get_secret_value('openweather_appid')
    logger.info(f"Invoking API with lat as {lat} and lon as {lon}")

    url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={appid}"

    try:
        response = requests.get(url)

        if response.status_code == 200:
            logger.debug("API Response {}".format(response.json()))
            return response.json()
        # elif response.status_code == 429:
        #     #rate limit exceeded , wait unit next day UTC
        #     set_global_variable("weathered_byte", "RATE_LIMIT_EXCEEDED", True)
        #     set_global_variable("weathered_byte", "RATE_LIMIT_EXCEEDED_DATE", value)

        #     return "RATE_LIMIT_EXCEEDED"
        else:
            logger.error(f"API Request Failed with status code: {response.status_code}, Message: {response.json()}")
            raise Exception(f"API Request Failed with status code: {response.status_code}, Message: {response.json()}")
    except requests.ConnectionError:
        logger.exception("API Connection Error. The URL may be down or incorrect.")
        raise
    except requests.Timeout:
        logger.exception("API Request timed out.")
        raise
    except requests.RequestException as e:
        logger.exception(f"An error occurred while making the API request: {e}")
        raise
    except Exception as e:
        logger.exception(f"An unspecified error occurred: {e}")
        raise


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'


@test
def test_schema(output, *args) -> None:
    # load file contents of response_schema.json
    with open('openweather_etl/data_loaders/response_schema.json') as f:
        schema_ow = json.load(f)
    validate_json(output, schema_ow)
    assert validate_json(output, schema_ow) is True, 'Output is valid'


def validate_json(data, schema):
    try:
        validate(instance=data, schema=schema)
        return True
    except ValidationError as e:
        print(f"JSON data is invalid: {e.message}")
        raise


#####################
#####################

#Transformer - json_transformer

import pandas as pd
import datetime
import math

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    # normalize
    normalized_df = normalize_raw_df(data)

    weather_df = normalize_weather_conditions(data)

    city_dict = transform_city(data)

    return {"main": normalized_df,
            "weather": weather_df,
            "city": city_dict
            }


def transform_city(data):
    city_dict = data['city']
    city_dict['sunrise'] = datetime.datetime.fromtimestamp(city_dict['sunrise'])
    city_dict['sunset'] = datetime.datetime.fromtimestamp(city_dict['sunset'])
    return city_dict


def normalize_weather_conditions(data):
    forecast_df = pd.DataFrame(data['list'])
    forecast_df['city_id'] = data['city']['id']
    weather_df = forecast_df[['dt', 'city_id', 'weather']]
    weather_df = weather_df.explode('weather')

    weather_norm_series = pd.json_normalize(weather_df['weather'])
    weather_df = pd.concat([weather_df.drop('weather', axis=1), weather_norm_series], axis=1)

    return weather_df


def epoch_to_str(epoch_timestamp):
    # Convert epoch to datetime in UTC
    utc_time = datetime.datetime.utcfromtimestamp(epoch_timestamp)

    return int(utc_time.strftime('%Y%m%d%H'))


def normalize_raw_df(data):
    result_df = pd.DataFrame(data['list'])

    main_norm_series = pd.json_normalize(result_df['main'])
    result_df = pd.concat([result_df.drop('main', axis=1), main_norm_series], axis=1)

    wind_norm_series = pd.json_normalize(result_df['wind'])
    wind_norm_series = wind_norm_series.rename(columns={c: f'wind_{c}' for c in wind_norm_series.columns})
    result_df = pd.concat([result_df.drop('wind', axis=1), wind_norm_series], axis=1)

    if 'rain' in result_df.columns:
        rain_norm_series = pd.json_normalize(result_df['rain'])
        rain_norm_series = rain_norm_series.rename(columns={c: f'rain_{c}' for c in rain_norm_series.columns})
        result_df = pd.concat([result_df.drop('rain', axis=1), rain_norm_series], axis=1)

    if 'clouds' in result_df.columns:
        clouds_norm_series = pd.json_normalize(result_df['clouds'])
        clouds_norm_series = clouds_norm_series.rename(columns={c: f'clouds_{c}' for c in clouds_norm_series.columns})
        result_df = pd.concat([result_df.drop('clouds', axis=1), clouds_norm_series], axis=1)

    if 'snow' in result_df.columns:
        snow_norm_series = pd.json_normalize(result_df['snow'])
        snow_norm_series = clouds_norm_series.rename(columns={c: f'snow_{c}' for c in clouds_norm_series.columns})
        result_df = pd.concat([result_df.drop('snow', axis=1), snow_norm_series], axis=1)

    result_df = result_df.drop('weather', axis=1)
    result_df = result_df.drop('sys', axis=1)

    timezone = data['city']['timezone']

    result_df['date_key'] = result_df['dt'].apply(epoch_to_str)
    result_df['city_id'] = data['city']['id']
    result_df['visibility'] = result_df['visibility'].apply(lambda x: 0 if math.isnan(x) else int(x))

    # Adding optional columns which might not be available in json response but need for database
    for opt_column in ['rain_1h', 'rain_2h', 'rain_3h', 'snow_1h', 'snow_2h', 'snow_3h', 'visibility', 'clouds_all']:
        if opt_column not in result_df.columns:
            result_df[opt_column] = float(0)

    return result_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['main'] is not None, 'Weather List is undefined'
    assert output['weather'] is not None, 'Weather Conditions is undefined'
    assert output['city'] is not None, 'City Info is missing'



#####################
#####################

#Data Exporter - batch_creator

from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path
from pandas import DataFrame

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def create_batch(data, **kwargs) -> DataFrame:
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    query = """INSERT INTO public.tbl_batches
    ( batch_name, batch_description, status, inserted_at, insert_user)
    VALUES('weather_forcast-etl', '', 'PROCESSING'::character varying, CURRENT_TIMESTAMP, 'elt_user') 
    RETURNING batch_id;"""

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        batch_df = loader.load(query)
        batch_id = batch_df.iloc[0, 0]
        loader.commit()

        return batch_id


#####################
#####################
#Data Exporter - city_exporter

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(batch_id: int, transformed_data: tuple, **kwargs) -> None:
    city_json = transformed_data['city']
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    finder_query = "select 1 from dim_city where city_id = {city_id}".format(city_id=city_json['id'])
    insert_query = """ 
        INSERT INTO public.dim_city
        (city_id, "name", country, latitude, longitude, timezone,batch_id, status)
        VALUES({city_id}, '{name}', '{country}', {lat}, {lon}, {timezone}, {batch_id}, 'ACTIVE'::character varying)
    """

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        df = loader.load(finder_query)
        if len(df) == 0:
            # insert into dim_city
            loader.execute(insert_query.format(city_id=city_json['id'],
                                               name=city_json['name'],
                                               country=city_json['country'],
                                               lat=city_json['coord']['lat'],
                                               lon=city_json['coord']['lon'],
                                               batch_id=batch_id,
                                               timezone=city_json['timezone']
                                               ))
            loader.commit()
        elif len(df) == 1:
            print("\nNo update needed")


#####################
#####################
#Data Exporter - weather_exporter


from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
import pandas as pd
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(data, batch_id: int, **kwargs) -> DataFrame:
    # return data
    # find unique weather conditions add to db
    weather_df = DataFrame(data['weather'])

    unique_weather_data = weather_df[['id', 'main', 'description']].drop_duplicates()
    unique_weather_data = unique_weather_data.rename(columns={"id": "weather_condition_id", "main": "name"})
    unique_weather_data['batch_id'] = batch_id
    unique_weather_data['status'] = 'ACTIVE'

    schema_name = 'public'
    table_name = 'dim_weather_condition'
    unique_constraints = ['weather_condition_id']  # Columns used to identify unique records
    unique_conflict_method = 'IGNORE'  # Method to resolve conflicts
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            unique_weather_data,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            allow_reserved_words=True,
            if_exists='APPEND',  # Specify resolution policy if table name already exists
            unique_constraints=unique_constraints,
            unique_conflict_method=unique_conflict_method
        )
        loader.commit()

    combined_df = weather_df.groupby(['dt'])['id'].apply(list).reset_index(name='weather_condition_id_list')
    weather_df = pd.merge(weather_df, combined_df, on='dt', how='inner')
    weather_df['weather_combination_id'] = weather_df['weather_condition_id_list'].apply(generate_combination_id)

    weather_combo_df = weather_df[['weather_combination_id', 'id']]
    weather_combo_df = weather_combo_df.rename(columns={'id': 'weather_condition_id'})
    weather_combo_df['batch_id'] = batch_id
    weather_combo_df['status'] = 'ACTIVE'

    table_name = 'dim_weather_combination'
    unique_constraints = ['weather_combination_id', 'weather_condition_id']  # Columns used to identify unique records
    unique_conflict_method = 'IGNORE'  # Method to resolve conflicts

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            weather_combo_df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            allow_reserved_words=True,
            if_exists='APPEND',  # Specify resolution policy if table name already exists
            unique_constraints=unique_constraints,
            unique_conflict_method=unique_conflict_method
        )
        loader.commit()

    return weather_df[['dt', 'weather_combination_id']]


def generate_combination_id(weather_condition_id_list) -> int:
    if (len(weather_condition_id_list) == 1):
        return weather_condition_id_list[0]
    return hash(tuple(weather_condition_id_list))

#####################
#####################
#Data Exporter - forecast_exporter


from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
import pandas as pd
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(weather_df, city_dummy, data, batch_id, **kwargs) -> None:
    forecast_df = DataFrame(data['main'])
    city = data['city']

    forecast_df['population'] = city['population']
    forecast_df['sunrise'] = city['sunrise']
    forecast_df['sunset'] = city['sunset']

    loader_df = pd.merge(forecast_df, weather_df, on='dt', how='inner')

    loader_df = loader_df[['city_id', 'date_key', 'population', 'sunrise', 'sunset', 'temp', 'feels_like', 'temp_min',
                           'temp_max', 'pressure', 'sea_level', 'grnd_level', 'humidity', 'temp_kf', 'wind_speed',
                           'wind_deg',
                           'wind_gust', 'clouds_all', 'visibility', 'rain_1h', 'rain_2h', 'rain_3h', 'snow_1h',
                           'snow_2h',
                           'snow_3h', 'weather_combination_id']]
    loader_df['batch_id'] = batch_id

    schema_name = 'public'  # Specify the name of the schema to export data to
    table_name = 'fact_forecast_staging'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    sql_query = f"""
    update fact_forecast set status='INACTIVE' 
    where (city_id, date_key) 
    IN (select city_id, date_key from fact_forecast_staging where batch_id = {batch_id} and status='ACTIVE');
    INSERT INTO public.fact_forecast
    (forecast_id, city_id, date_key, population, sunrise, sunset, "temp", feels_like, 
    temp_min, temp_max, pressure, sea_level, grnd_level, 
    humidity, temp_kf, wind_speed, wind_deg, wind_gust, clouds_all, 
    visibility, rain_1h, rain_2h, rain_3h, snow_1h, snow_2h, snow_3h, weather_combination_id, batch_id, status)
    SELECT forecast_id, city_id, date_key, population, sunrise, sunset, 
    "temp", feels_like, temp_min, temp_max, pressure, sea_level, grnd_level, 
    humidity, temp_kf, wind_speed, wind_deg, wind_gust, clouds_all, visibility, 
    rain_1h, rain_2h, rain_3h, snow_1h, snow_2h, snow_3h, weather_combination_id, batch_id, status
    FROM public.fact_forecast_staging where batch_id={batch_id};
    delete from public.fact_forecast_staging where batch_id={batch_id};
        """

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            loader_df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            allow_reserved_words=True,
            if_exists='APPEND'  # Specify resolution policy if table name already exists
        )
        loader.execute(sql_query)
        loader.commit()


#####################
#####################
#Data Exporter - close_batch

from openweather_etl.utils.batch_updater import update_batch

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(d1: None,batch_id: int, **kwargs) -> None:
    update_batch(batch_id, 'DONE')


### batch_updater.py

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path


def update_batch(batch_id: int, status: str) -> None:
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    query = f"""update public.tbl_batches set updated_at = CURRENT_TIMESTAMP, update_user='etl_user',
     status='{status}' where batch_id = {batch_id}"""

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
       loader.execute(query)
       loader.commit()
