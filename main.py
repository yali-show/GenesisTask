from config import *
import logging
import json
import datetime
import time
import io
import requests
import pandas as pd
import pyarrow.parquet as pq
from google.cloud import storage, bigquery


class ApiControl:
    """Class for control api methods"""

    def __init__(self, url, key):
        self.url = url
        self.key = key

    def get_installs(self, date: str) -> dict:
        """
        :param date: string yyyy-mm-dd
        :return: response from api
        """

        try:
            request = requests.get(url=self.url + '/installs',
                                   headers=self.key,
                                   params={'date': date})
            result = request.json()
            return result

        except Exception as ex:
            logging.error("Something went wrong in 'get_installs'")
            print(ex)

    def get_costs(self, date: str, dimension=None) -> requests.Response:
        """

        :param date: string yyyy-mm-dd
        :param dimension: (str) location, channel, medium, campaign, keyword,
         ad_content, ad_group, landing_page
        :return: response from api
        """

        params = {'date': date}

        if dimension:
            params['dimensions'] = dimension

        logging.info("Costs request getting")

        try:
            request = requests.get(url=self.url + '/costs',
                                   headers=self.key,
                                   params=params)
            return request

        except Exception as ex:
            logging.error("Something went wrong in 'ApiControl.get_costs'")
            print(ex)

    def get_orders(self, date: str) -> requests.Response:
        logging.info("Orders response getting")
        try:
            request = requests.get(url=self.url + '/orders',
                                   headers=self.key,
                                   params={'date': date})
            return request

        except Exception as ex:
            logging.error('Something went wrong in "ApiControl.get_orders"')
            print(ex)

    def get_events_next_page(self, date: str, next_page=None):
        params = {'date': date}

        if next_page:
            params["next_page"] = next_page

        request = requests.get(url=self.url + '/events',
                               headers=self.key,
                               params=params)
        return request.json()


class GCP:
    def __init__(self, url, key):
        self.api_cursor = ApiControl(url, key)
        self.FILE = 'DataMart_data/DataMart.csv'
        self.PROJECT_NAME = 'hly-gnss'
        self.TABLE_NAMES = ['cpi', 'revenue', 'roas']
        self.BUCKET_NAMES = 'hly-gnss-bckt'
        self.DATE = ((datetime.datetime.now() - datetime.timedelta(days=1))
                     .strftime('%Y-%m-%d'))

        self.data_frame_for_update = self.setup_existed_data()

        self.installs = self.api_cursor.get_installs(self.DATE)['count']

    def setup_existed_data(self) -> pd.DataFrame:
        client = storage.Client()
        bucket = client.get_bucket(self.BUCKET_NAMES)
        blob = bucket.blob(self.FILE)

        if blob.exists():
            file_content = blob.download_as_string()
            data = io.StringIO(file_content.decode('utf-8'))
            df = pd.read_csv(data)
            return df
        else:
            return pd.DataFrame()

    def orders_prepare(self):

        parquet_table = pq.read_table(
            io.BytesIO(self.api_cursor.get_orders(self.DATE).content)
        )
        df = parquet_table.to_pandas()
        return df

    def costs_prepare(self) -> float:
        costs = self.api_cursor.get_costs(self.DATE).content
        data = costs.decode('utf-8')
        data = data.split('\n')
        costs = float(data[1])
        return costs

    def events_prepare(self):
        # TODO change logic

        in_progres = True
        page = 0
        errors = 10
        output = self.api_cursor.get_events_next_page(self.DATE)
        next_page = output['next_page']
        output = output['data']

        while in_progres:

            try:
                page += 1
                print(page)

                # print(output)
                output = pd.DataFrame(output)
                print(output)
                logging.info("Trying to get response data")
                output = self.api_cursor.get_events_next_page(self.DATE,
                                                         next_page)

                logging.info("Trying to get new page's link")
                next_page = output['next_page']

            except requests.exceptions.JSONDecodeError as json_ex:
                page -= 1
                logging.error("Json decode error")
                errors -= 1

                if errors == 0:
                    in_progres = False

                time.sleep(5)

            except KeyError as krr:

                logging.warning("The last one page was shared or api changed",
                                exc_info=krr)

                in_progres = False

    def cpi_data(self) -> int:
        costs = self.costs_prepare()
        data = costs / self.installs
        return data

    def revenue_data(self) -> int:
        ...

    def roas_data(self) -> int:
        ...

    def new_data(self) -> None:
        logging.info("Setup data for update")
        try:
            new_data = {'date': [self.DATE],
                        'cpi': [self.cpi_data()],
                        'revenue': [self.revenue_data()],
                        'roas': [self.roas_data()]}

            pd.concat([self.data_frame_for_update, new_data])
        except Exception as ex:
            logging.error("Something went wrong in 'GCP.new_data'")
            print(ex)

    def upload_changes(self) -> None:
        self.new_data()

        logging.info("Connecting to storage")

        try:
            client = storage.Client()
            bucket = client.get_bucket(self.BUCKET_NAMES)
            blob = bucket.blob(self.FILE)
            df = self.data_frame_for_update.to_csv(index=False)
            blob.upload_from_string(df, 'text/csv')
        except Exception as ex:
            logging.error("Something went wrong in 'GCP.upload_changes'")
            print(ex)


if __name__ == '__main__':
    controller = ApiControl(URL, KEY)
    # data = {'date': [1], 'instals': [2], 'costs': [3], 'cpi': [4]}
    # df = pd.DataFrame(data)
    # print(df.to_csv())
    data = {'date': ['2023-12-16', '2023-12-17'],
            'cpi': [2222, 4434],
            'revenue': [3333, 23434.333],
            'roas': [234324, 345345]}

    # Создание датафрейма
    # df = pd.DataFrame(data)
    # df = pd.DataFrame()
    # print(df)
    # new_data = {'date': ['2023-12-18'],
    #         'cpi': [1],
    #         'revenue': [1],
    #         'roas': [1]}
    # new_data = pd.DataFrame(new_data)
    # print(pd.concat([df, new_data]))
    #




