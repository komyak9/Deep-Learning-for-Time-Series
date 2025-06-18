import pandas as pd
import numpy as np
import os
import openmeteo_requests
import requests_cache
from retry_requests import retry


def read_from_dir_as_df(dir_path: str, file_format: str=".csv") -> pd.DataFrame:
    """
    Read all files in a directory with a specified file format and return them as a single pandas DataFrame.
    """
    if not os.path.isdir(dir_path):
        raise ValueError(f"Directory {dir_path} doesn't exist.")
    
    files = [fn for fn in os.listdir(dir_path) if fn.endswith(file_format)]
    dataframes = []
    for file in files:
        match file_format:
            case ".csv":
                df = pd.read_csv(os.path.join(dir_path, file))
            case ".xlsx":
                df = pd.read_excel(os.path.join(dir_path, file))
            case _:
                raise ValueError(f"Unsupported file format: {file_format}")
        dataframes.append(df)
    return pd.concat(dataframes).reset_index(drop=True)



def load_raw_data_from_dir() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load data from the 'data' directory and return it as a pandas DataFrame.
    Data includes electricity price, production, and consumption.
    """
    raw_data_dir = "../data/raw/"

    prices_df = load_raw_prices(raw_data_dir)
    cons_df = load_raw_consumption(raw_data_dir)
    prod_df = load_raw_production(raw_data_dir)
    merged_data = pd.merge(prices_df, cons_df, on=["start_ts_utc", "end_ts_utc"], how="left")
    merged_data = pd.merge(merged_data, prod_df, on=["start_ts_utc", "end_ts_utc"], how="left")
    merged_data.replace(["n/e", "-"], np.nan, inplace=True)
    return merged_data



def load_raw_prices(raw_data_dir: str) -> pd.DataFrame:
    """
    Load electricity price data from the 'data/raw/prices' directory.
    Preprocess data initially.
    """
    prices_df = read_from_dir_as_df(dir_path=os.path.join(raw_data_dir, "prices"))

    prices_df = prices_df[prices_df.Sequence=="Sequence Sequence 1"]
    prices_df.rename(
        columns={
            "Day-ahead Price (EUR/MWh)": "da_price_eur_mwh"
        },
        inplace=True
    )
    prices_df[["start_ts_utc", "end_ts_utc"]] = prices_df["MTU (UTC)"].str.split(" - ", expand=True).apply(
        pd.to_datetime, 
        format="%d/%m/%Y %H:%M:%S", 
        utc=True
        )

    columns_to_select = ["start_ts_utc", "end_ts_utc", "da_price_eur_mwh"]
    return prices_df[columns_to_select].sort_values(by=["start_ts_utc"]).reset_index(drop=True)

def load_raw_consumption(raw_data_dir: str) -> pd.DataFrame:
    """
    Load electricity consumption data from the 'data/raw/consumption' directory.
    Preprocess data initially.
    """
    cons_df = read_from_dir_as_df(dir_path=os.path.join(raw_data_dir, "consumption"))

    cons_df[["start_ts_utc", "end_ts_utc"]] = cons_df["MTU (UTC)"].str.split(" - ", expand=True).apply(
        pd.to_datetime, 
        format="%d/%m/%Y %H:%M", 
        utc=True
        )
    cons_df.rename(
        columns={
            "Actual Total Load (MW)": "actual_load_mw"
        },
        inplace=True
    )

    columns_to_select = ["start_ts_utc", "end_ts_utc", "actual_load_mw"]
    return cons_df[columns_to_select].sort_values(by=["start_ts_utc"]).reset_index(drop=True)

def load_raw_production(raw_data_dir: str) -> pd.DataFrame:
    """
    Load electricity production data from the 'data/raw/production' directory.
    Preprocess data initially.
    """
    prod_df = read_from_dir_as_df(dir_path=os.path.join(raw_data_dir, "production"))

    prod_df[["start_ts_utc", "end_ts_utc"]] = prod_df["MTU (UTC)"].str.split(" - ", expand=True).apply(
        pd.to_datetime, 
        format="%d/%m/%Y %H:%M:%S", 
        utc=True
        )
    prod_df.rename(
        columns={
            "Generation (MW)": "actual_generation_mw",
            "Production Type": "production_type"
        },
        inplace=True
    )
    renewable_types = ['Solar', 'Wind Offshore', 'Wind Onshore']
    prod_df = prod_df[prod_df["production_type"].isin(renewable_types)]

    columns_to_select = ["start_ts_utc", "end_ts_utc", "production_type", "actual_generation_mw"]
    prod_df = prod_df[columns_to_select].sort_values(by=["production_type", "start_ts_utc"]).reset_index(drop=True)

    prod_df = prod_df.pivot(index=["start_ts_utc", "end_ts_utc"], columns="production_type", values="actual_generation_mw").reset_index()
    prod_df.columns = list(prod_df.columns[:2])+["actual_generation_mw_"+col.lower().replace(" ", "_") for col in prod_df.columns[2:]]
    return prod_df



def load_and_save_raw_weather_forecast():
    coordinate_to_city = {
        (52.5200, 13.4050): "Berlin",
        (53.5511, 9.9937): "Hamburg",
        (48.1351, 11.5820): "Munich",
        (50.9375, 6.9603): "Cologne",
        (50.1109, 8.6821): "Frankfurt",
        (51.3397, 12.3731): "Leipzig",
        (48.7758, 9.1829): "Stuttgart",
        (54.3233, 10.1228): "Kiel",
        (49.4521, 11.0767): "Nuremberg",
        (47.9990, 7.8421): "Freiburg",
    }

    fcst_dfs = []
    for coord_pair, city_name in zip(coordinate_to_city.keys(), coordinate_to_city.values()):
        extr_forecast = load_raw_weather_forecast_from_om(coordinates=tuple(coord_pair))
        extr_forecast['city'] = city_name
        fcst_dfs.append(extr_forecast)
    fcst_df = pd.concat(fcst_dfs, ignore_index=True).sort_values(by=["city", "datetime_utc"]).reset_index(drop=True)
    fcst_df.to_csv("../data/raw/weather_forecast/weather_forecast_10p_germany.csv", index=False)



def load_raw_weather_forecast_from_om(
        coordinates: tuple[float, float],
        ) -> pd.DataFrame:
    """
    Load raw weather forecast data from Open Meteo API.
    """
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"

    latitudes, longitudes = zip(coordinates)
    weather_params = [
        "temperature_2m", "dew_point_2m", "relative_humidity_2m", "rain", "showers", "snowfall", "snow_depth",
        "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
        "wind_speed_10m", "wind_speed_120m", "wind_speed_80m", "wind_speed_180m",
        "wind_direction_10m", "wind_direction_80m", "wind_direction_180m", "wind_direction_120m",
        "wind_gusts_10m", "direct_radiation", "diffuse_radiation", "shortwave_radiation"
        ]
    params = {
        "latitude": latitudes,
        "longitude": longitudes,
        "start_date": "2023-01-01",
        "end_date": "2025-06-13",
        "hourly": weather_params,
        "models": "icon_seamless",
        "timezone": "GMT"
    }
    responses = openmeteo.weather_api(url, params=params)

    forecasts_dfs = []
    for response in responses:
        # Process hourly data. The order of variables needs to be the same as requested.
        hourly = response.Hourly()

        hourly_data = {"datetime_utc": pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
            end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
        )}

        hourly_data["latitude"] = round(response.Latitude(), 2)
        hourly_data["longitude"] = round(response.Longitude(), 2)
        for i in range(len(weather_params)):
            hourly_data[weather_params[i]] = hourly.Variables(i).ValuesAsNumpy()

        forecasts_dfs.append(pd.DataFrame(data = hourly_data))
    return pd.concat(forecasts_dfs, ignore_index=True)