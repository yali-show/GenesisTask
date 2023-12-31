from config import *
import base64
import logging
import datetime
import io

import pandas.errors
import requests
import pandas as pd
import pyarrow.parquet as pq
from google.cloud import storage


class ApiControl:
    """Class for control api methods"""

    def __init__(self, url, key):
        self.url = url
        self.key = key

    def get_installs(self, date: str) -> dict:
        """
        Instals info
        :param date: string yyyy-mm-dd
        :return: dict response from api
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
        Managment info
        :param date: string yyyy-mm-dd
        :param dimension: (str) location, channel, medium, campaign, keyword,
         ad_content, ad_group, landing_page
        :return: requests.Response from server
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
        """
        Orders info
        :param date: string yyyy-mm-dd
        :return: requests.Response from server
        """
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
        """
        Users events info
        :param date: string yyyy-mm-dd
        :param next_page: string next_page token
        :return:
        """
        params = {'date': date}

        if next_page:
            params["next_page"] = next_page

        request = requests.get(url=self.url + '/events',
                               headers=self.key,
                               params=params)
        return request.json()


class GCP:
    """Preparing data and uploading to GCP"""
    def __init__(self,
                 url=URL,
                 key=KEY):

        self.api_cursor = ApiControl(url, key[0])
        self.FILE = 'DataMart_data/DataMart.csv'
        self.PROJECT_NAME = 'hly-gnss'
        self.TABLE_NAME = ['cpi', 'revenue', 'roas']
        self.BUCKET_NAMES = 'hly-gnss-bckt'
        self.DATE = ((datetime.datetime.now() - datetime.timedelta(days=1))
                     .strftime('%Y-%m-%d'))

        self.data_frame_for_update = self.setup_existed_data()

    def setup_existed_data(self) -> pd.DataFrame:
        """
        Upload from GCP existed csv to pandas or create new DataFrame obj
        :return: pd.DataFrame DataMart
        """
        client = storage.Client()
        bucket = client.get_bucket(self.BUCKET_NAMES)
        blob = bucket.blob(self.FILE)

        not_exist_or_empty = blob.exists()

        if not_exist_or_empty:
            try:
                file_content = blob.download_as_string()
                data = io.StringIO(file_content.decode('utf-8'))
                df = pd.read_csv(data)
                return df

            except pandas.errors.EmptyDataError:
                logging.warning("Empty data in the DataMart")
                return pd.DataFrame()
        else:
            return pd.DataFrame()

    def new_data(self) -> None:
        """ Prepare new data for DataMart updating """
        logging.info("Updating data prepare")
        try:
            costs = self.costs_prepare()
            revenue = self.revenue_data()
            new_data = {'date': [self.DATE],
                        'cpi': [self.cpi_data(costs)],
                        'revenue': [revenue],
                        'roas': [self.roas_data(revenue, costs)]}

            new_data = pd.DataFrame(new_data)

            self.data_frame_for_update = pd.concat([self.data_frame_for_update, new_data])
        except Exception as ex:
            logging.error("Something went wrong in 'GCP.new_data'")
            print(ex)

    def upload_changes(self) -> None:
        """ Update DataMart """

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

    def orders_prepare(self) -> pd.DataFrame:
        """
        Prepare orders dataset
        :return: pd.DataFrame with orders data
        """
        try:
            orders = self.api_cursor.get_orders(self.DATE).content

            coded_orders = io.BytesIO(orders)

            parquet_table = pq.read_table(coded_orders)
            df = parquet_table.to_pandas()
            return df

        except Exception as ex:
            logging.error("Orders prepare error")

    def costs_prepare(self) -> float:
        """
        Get managment costs info
        :return: float costs (managment)
        """
        costs = self.api_cursor.get_costs(self.DATE).content
        data = costs.decode('utf-8')
        data = data.split('\n')
        costs = float(data[1])
        return costs

    def cpi_data(self, costs) -> float:
        """
        Get cpi value
        :return: float cpi
        """
        installs = self.api_cursor.get_installs(self.DATE)['count']
        cpi = costs / installs
        return cpi

    def revenue_data(self) -> float:
        """
        Get revenue value
        :return: float revenue
        """
        orders = self.orders_prepare()
        revenue = (orders['iap_item.price'].sum() - orders['tax'].sum()
                   - orders['fee'].sum() - orders['discount.amount'].sum())
        return revenue

    @staticmethod
    def roas_data(revenue, costs) -> float:
        """
        Calculate roas value
        :return: float roas value
        """
        return revenue / costs


def run_the_code(event, content):
    pubsub_ms = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_ms)
    connector = GCP()
    connector.upload_changes()


if __name__ == '__main__':
    run_the_code('event', 'content')
