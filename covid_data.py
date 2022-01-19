import pandas as pd
from datetime import datetime, timedelta
from gspread_pandas import Spread

from airflow import DAG
from airflow.decorators import task


with DAG(
    dag_id="covid_dashboard_operator",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 19, 14),
    catchup=True,
) as dag:

    @task(task_id="covid_data_dag")
    def covid_data_dag():

        try:
            covid_data_raw = pd.read_csv(
                "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv",
                parse_dates=["date"],
            )
            print("Successfully loaded data from source")

        except Exception as e:
            raise Exception(
                "Unable to load data from source, using latest days data: \n\n %s" % e
            )

        try:
            """
            Cleans data by removing uneeded columns and dropping continents.
            Also interpolates vaccination data due to inconsitencies each day.
            """

            covid_data_raw_continent = covid_data_raw.loc[
                ~covid_data_raw["continent"].isna(), :
            ]

            regex = """total|excess|aged|extreme|capita|cardiovasc|\
                |smokers|tests|index|expectancy|beds|density|\
                    |handwashing|diabetes|median|positive|reproduction|\
                        |iso_code|continent|weekly|per_million|per_hundred|smoothed"""

            dropped_columns = list(covid_data_raw_continent.filter(regex=regex))

            booster_column = ["total_boosters"]

            covid_data = covid_data_raw_continent.copy()[
                covid_data_raw_continent.columns.drop(dropped_columns)
            ]

            covid_data[booster_column] = covid_data_raw_continent[booster_column]

            interp_columns = [
                "people_fully_vaccinated",
                "people_vaccinated",
                "total_boosters",
            ]

            covid_data_interp = pd.DataFrame()

            locations = covid_data["location"].unique()

            for location in locations:
                location_set = covid_data.copy().loc[
                    covid_data["location"] == location, :
                ]
                location_set = location_set.sort_values(["location", "date"])
                location_set[interp_columns] = location_set[
                    interp_columns
                ].interpolate()
                location_set.iloc[:, 2:] = location_set.iloc[:, 2:].fillna(0)
                covid_data_interp = covid_data_interp.copy().append(location_set)

            print("Successfully cleaned data")

        except Exception as e:
            raise Exception("Unable to clean data, using latest days data \n %s" % e)

        try:
            """
            Uploads data to Google Sheets and schedules the next run for 4pm the following day.
            """

            spread = Spread("COVID Dashboard")
            spread.df_to_sheet(
                covid_data_interp,
                index=False,
                sheet="Dashboard Data",
                start="A1",
                replace=True,
            )

        except Exception as e:
            raise Exception(
                "Unable to load data to Google Sheets, using prior days data \n %s" % e
            )

    covid_data_pipeline = covid_data_dag()
