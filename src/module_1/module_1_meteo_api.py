import json
import logging
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import requests
import time


logger = logging.getLogger(__name__)

logger.level = logging.INFO
# flake8: noqa E501
# import time
# import pandas

API_URL = "https://climate-api.open-meteo.com/v1/climate?"
COORDINATES = {
    "Madrid": {"latitude": 40.416775, "longitude": -3.703790},
    "London": {"latitude": 51.507351, "longitude": -0.127758},
    "Rio": {"latitude": -22.906847, "longitude": -43.172896},
}
VARIABLES = "temperature_2m_mean,precipitation_sum,soil_moisture_0_to_10cm_mean"
MODELS = (
    "CMCC_CM2_VHR4,FGOALS_f3_H,HiRAM_SIT_HR," "MRI_AGCM3_2_S,EC_Earth3P_HR,MPI_ESM1_2_XR,NICAM16_8S"
)


def get_data_meteo_api(city, start_date, end_date):
    """
    Get API url based on city, start_date and end date
    """

    city_dict = COORDINATES[city]
    latitude = city_dict["latitude"]
    longitude = city_dict["longitude"]

    api_url = (
        f"https://climate-api.open-meteo.com/v1/climate?"
        f"latitude={latitude}&longitude={longitude}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&models={MODELS}"
        f"&daily={VARIABLES}"
    )

    return request_with_cooloff(api_url)


def _request_with_cooloff(api_url, num_attempts):
    cooloff = 1

    for call_count in range(cooloff):
        try:
            response = requests.get(api_url)
            response.raise_for_status()

        except requests.exceptions.ConnectionError as e:
            logger.info("API refused the connection")
            logger.warning(e)
            if call_count != (num_attempts - 1):
                time.sleep(cooloff)
                cooloff *= 2
                call_count += 1
                continue
            else:
                raise

        except requests.exceptions.HTTPError as e:
            logger.warning(e)
            if response.status_code == 404:
                raise

            logger.info(f"API return code {response.status_code} cooloff at {cooloff}")
            if call_count != (num_attempts - 1):
                time.sleep(cooloff)
                cooloff *= 2
                call_count += 1
                continue
            else:
                raise

        # We got through the loop without error so we've received a valid response
        return response


def request_with_cooloff(url, num_attempts=10):
    return json.loads(_request_with_cooloff(url, num_attempts).content.decode("utf-8"))


def get_yearly_mean_std(data_dict, variable_dict):
    """
    Get a df with yearly mean and std for every variable
    """

    df = pd.DataFrame(data_dict["daily"], index=pd.to_datetime(data_dict["daily"]["time"]))
    df.drop(columns="time", inplace=True)

    for variable in variable_dict.keys():
        variable_columns = [column for column in df.columns if variable in column]
        df[variable] = df[variable_columns].mean(axis=1)
        df.drop(columns=variable_columns, inplace=True)

    return df.groupby(df.index.year).agg(["mean", "std"])


def plot_data(df, variable_dict, city):
    """
    Plot variables for every city
    """

    fig, axes = plt.subplots(3, 1, figsize=(12, 8))

    for axe, variable, unit in zip(axes, variable_dict.keys(), variable_dict.values()):
        init_year = df.index[0]
        end_year = df.index[-1]
        axe.set_title(variable)
        axe.set_ylabel(unit)

        axe.plot(df[variable]["mean"], "-b", label="mean")
        axe.plot(df[variable]["mean"] + df[variable]["std"], "--r", label="std")
        axe.plot(df[variable]["mean"] - df[variable]["std"], "--r")
        axe.set_xticks([])
        axe.legend()

    custom_xticks = [year for year in range(init_year, end_year, 5)]  # x label with two years spans

    custom_xticks_labels = [str(year) for year in custom_xticks]  # x label with two years spans

    axes[-1].set_xticks(custom_xticks)
    axes[-1].set_xticklabels(labels=custom_xticks_labels)
    plt.xticks(fontsize=10)
    plt.xticks(rotation=45)

    # Modify additional parameters for the entire figure
    fig.suptitle(city, fontsize=16)
    plt.savefig(f"src/module_1/{city}.png")
    plt.close()


def main():
    start_date = "1950-01-01"
    end_date = "2049-12-31"

    cities = COORDINATES.keys()

    for city in cities:
        data_dict = get_data_meteo_api(city, start_date, end_date)

        variable_dict = {}

        for variable in VARIABLES.split(","):
            value = next(
                (value for key, value in data_dict["daily_units"].items() if variable in key), None
            )
            variable_dict[variable] = value
            # 'temperature_2m_mean': '°C'

        df = get_yearly_mean_std(data_dict, variable_dict)
        plot_data(df, variable_dict, city)


if __name__ == "__main__":
    main()
