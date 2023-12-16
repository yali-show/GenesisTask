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

        request = requests.get(url=self.url + '/installs',
                               headers=self.key,
                               params={'date': date})
        result = request.json()
        return result

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

        request = requests.get(url=self.url + '/costs',
                               headers=self.key,
                               params=params)
        return request

    def get_orders(self, date: str) -> requests.Response:

        request = requests.get(url=self.url + '/orders',
                               headers=self.key,
                               params={'date': date})
        return request

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
        self.project_name = 'hly-gnss'
        self.tables_names = ['CPI', 'Revenue', 'ROAS']
        self.bucket_name = 'hly-gnss-bckt'
        self.date = ((datetime.datetime.now() - datetime.timedelta(days=1))
                     .strftime('%Y-%m-%d'))

    def installs_prepare(self):
        # TODO change date
        # TODO change csv return

        installs_count = self.api_cursor.get_installs(self.date)['count']
        # installs_records = self.api_cursor.get_installs(self.date)['records']
        # normalized_installs = installs_records.replace("'", '"')
        # normalized_installs = json.loads(normalized_installs)
        # df = pd.DataFrame.from_dict(normalized_installs)
        #
        # return df

    def orders_prepare(self):
        # TODO change date
        # TODO change csv return

        parquet_table = pq.read_table(
            io.BytesIO(controller.get_orders(self.date).content)
        )
        df = parquet_table.to_pandas()
        return df

    def costs_prepare(self):
        # TODO add more parametrs
        # TODO change csv return

        costs = self.api_cursor.get_costs(self.date).content
        costs = costs.decode('utf-8')
        rows = costs.split('\n')
        columns = rows[0].split('\t')

        data = {}

        for row in rows[1:]:
            values = row.split('\t')
            if len(values) == 2:
                channel = values[0]
                cost = float(values[1])
                data[channel] = cost

        df = pd.DataFrame.from_dict(data, orient='index', columns=[columns[1]])

    def events_prepare(self):
        # TODO change logic

        in_progres = True
        page = 0
        errors = 10
        output = controller.get_events_next_page(self.date)
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
                output = controller.get_events_next_page(self.date,
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

    def cpi_create_data(self):
        data = {'date': self.date, 'instals': self.instals, 'costs': self.costs,
                'cpi': self.costs/self.instals}


if __name__ == '__main__':
    controller = ApiControl(URL, KEY)
    print(controller.get_installs("2020-12-10"))


