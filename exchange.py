import pandas as pd
import pandas_gbq
import requests
import io
import json
import os

from datetime import datetime, timedelta
from google.cloud import bigquery


class API:

    @staticmethod
    def count_rate(curr, value, date, table_name, credentials):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials
        client = bigquery.Client()
        query_job = client.query(
            f"""
            SELECT OBS_VALUE, SOURCE FROM `{table_name}` 
            WHERE TIME_PERIOD = '{date}' and CURRENCY = '{curr}';
        """
        )

        return [f"{float(value) * row['OBS_VALUE']} by {row['SOURCE']}" for row in query_job]

    @staticmethod
    def validate(curr, value, date):
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            # raise ValueError("Incorrect data format, date should be YYYY-MM-DD")
            return "Incorrect data format, date should be YYYY-MM-DD"
        try:
            float(value)
        except ValueError:
            # raise ValueError("Incorrect data format, value should be float eg. 12.1")
            return "Incorrect data format, value should be float eg. 12.1"
        try:
            if not (curr.isalpha() and len(curr) == 3):
                raise ValueError
        except ValueError:
            # raise ValueError("Incorrect data format, currency should be XYZ")
            return "Incorrect data format, currency should be XYZ"


class Exchange_rate():

    def __init__(self, table_name, credentials, currency='EUR', init_date=datetime.today(), num_of_days=7):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials
        self.EXR = pd.DataFrame(columns=['CURRENCY', 'CURRENCY_DENOM', 'TIME_PERIOD', 'OBS_VALUE', 'SOURCE'])
        self.main_currency = currency
        self.restore = False
        self.table = table_name
        self.project_id = self.table.split(".", 1)[0]
        self.table_id = self.table.split(".", 1)[1]
        self.num_of_days = num_of_days
        self.init_date = init_date

        self.set_dates(self.init_date)


    def set_dates(self, date):
        if type(date) is str:
            self.init_date = datetime.strptime(date, "%Y-%m-%d")
        self.now_raw = self.init_date
        self.day_to_del = (self.now_raw - timedelta(days=self.num_of_days)).strftime("%Y-%m-%d")
        self.now = self.now_raw.strftime("%Y-%m-%d")


    def del_from_BQ(self, source, date):

        client = bigquery.Client()
        query_job = client.query(
            f"""
            DELETE FROM `{self.table}` 
            WHERE TIME_PERIOD = '{date}' AND SOURCE = '{source}';
            """
        )


    def fill_day_from_BQ(self, source):

        client = bigquery.Client()
        query_job = client.query(
            f"""
            DELETE FROM `{self.table}` 
            WHERE TIME_PERIOD = '{self.now}' AND SOURCE = '{source}';
            
            INSERT INTO `{self.table}` 
            (
            CURRENCY, 
            CURRENCY_DENOM, 
            TIME_PERIOD, 
            OBS_VALUE, 
            SOURCE
            )
            SELECT 
                CURRENCY, 
                CURRENCY_DENOM, 
                DATE('{self.now}') as TIME_PERIOD, 
                OBS_VALUE, 
                SOURCE
            FROM `{self.table}`
            WHERE TIME_PERIOD = '{(self.now_raw - timedelta(days=1)).strftime("%Y-%m-%d")}'
            AND SOURCE = '{source}';
            """
        )


    def restore_table(self, first_day):
        first_day = datetime.strptime(first_day, "%Y-%m-%d")
        days = [first_day + timedelta(days=x) for x in range(self.num_of_days)]
        self.restore = True
        for day in days:
            self.set_dates(day)
            self.daily_update()

        #restore init date
        self.set_dates(self.init_date)
        self.restore = False


    def daily_update(self):
        list_of_sources = [source for source in Exchange_rate.__dict__ if source.endswith('_source_update')]
        # print(*list_of_sources, sep='\n')  # list of actual implemented APIs

        for func in list_of_sources:
            getattr(self, func)()


    def ebc_source_update(self):
        request_url = f'https://sdw-wsrest.ecb.europa.eu/service/data/EXR/D..{self.main_currency}.SP00.A'
        source = 'ebc'

        parameters = {
            'startPeriod': self.now,
            'endPeriod': self.now,
            'detail': 'dataonly'
        }

        response = requests.get(request_url, params=parameters, headers={'Accept': 'text/csv'})

        if len(response.text) != 0 and response.status_code == 200:
            df = pd.read_csv(io.StringIO(response.text))
            df['SOURCE'] = source
            # df['TIME_PERIOD'] = pd.to_datetime(df['TIME_PERIOD'])
            df = df[['CURRENCY', 'CURRENCY_DENOM', 'TIME_PERIOD', 'OBS_VALUE', 'SOURCE']]

            self.del_from_BQ(source, self.now)
            pandas_gbq.to_gbq(df, self.table_id, project_id=self.project_id, api_method="load_csv", if_exists='append')

            print(f"Filling data from API | date: {self.now} | ebc_update")

            if not self.restore:
                self.del_from_BQ(source, self.day_to_del)

        elif len(response.text) == 0 and response.status_code == 200:
            print(f"Filling data from BQ | date: {self.now} | ebc_update")
            self.fill_day_from_BQ(source)
            if not self.restore:
                self.del_from_BQ(source, self.day_to_del)
        else:
            print(f"ebc_update wrong API answer {response.status_code} | date: {self.now} ")


    def fcapi_source_update(self):
        source = 'fcapi'
        request_url = f"https://api.freecurrencyapi.com/v1/latest"
        parameters = {
            'apikey': 'jETABYyrZvUCnK0Lb7NpAzIwvc2r1rM5XHGdzxan',
            'currencies': '',
            'base_currency': 'EUR'
        }

        response = requests.get(request_url, params=parameters, headers={'Accept': 'application/json'})

        if response.status_code == 200:
            df = json.dumps(response.json()['data'])

            df = pd.read_json(df, orient="index")
            df.columns = ['OBS_VALUE']
            df = df.reset_index().rename(columns={'index': 'CURRENCY'})
            df["CURRENCY_DENOM"] = self.main_currency
            df["TIME_PERIOD"] = self.now
            df["SOURCE"] = source

            print(f"Filling data from API | date: {self.now} | fcapi_update")

            self.del_from_BQ(source, self.now)
            pandas_gbq.to_gbq(df, self.table_id, project_id=self.project_id, api_method="load_csv", if_exists='append')

            if not self.restore:
                self.del_from_BQ(source, self.day_to_del)
        else:
            print(f"fcapi_update wrong API answer {response.status_code} | date: {self.now} ")


if __name__ == "__main__":

    # First example  run
    updater = Exchange_rate(table_name='<project_id>.<dataset>.<table_name>',
                            credentials='credentials.json',
                            init_date='2022-12-20')  # without init_date= take today actual date

    updater.daily_update()
