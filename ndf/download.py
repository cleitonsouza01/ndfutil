# NDF download manager
import pathlib
from datetime import date, datetime, timedelta
import os
import sys

import pandas as pd
import joblib
import holidays
import requests as requests
from loguru import logger

from ndf.table import Table
from scrapy.http import TextResponse

logger.add(sys.stderr, format="{time} {level} {message}", filter="my_module", level="DEBUG")
# logger.add(sys.stdout, colorize=True, format="<green>{time}</green> <level>{message}</level>")
logger.add("ndfdownload.log", rotation="30 MB")


class ndf:
    def __init__(self):
        self.year_size = 325
        self.data_dir_name = 'DATA'
        self.BGC_result = None
        self.TRADITION_result = None
        self.TULLETPREBON_result = None
        self.GFI_result = None
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/50.0.2661.102 Safari/537.36'}

    def is_weekday(self, date):
        weekno = date.weekday()
        result = None
        if weekno < 5:
            result = True
        else:
            # 5 Sat, 6 Sun
            result = False
        return result

    def is_holiday(self, date):
        us_holidays = holidays.NYSE()
        br_holidays = holidays.BR()
        if (date.date() in us_holidays) or (date.date() in br_holidays):
            return True
        else:
            return False

    def is_yesterday_weekday(self):
        yesterday = datetime.today() - timedelta(days=1)
        weekno = yesterday.weekday()
        result = None
        if weekno < 5:
            result = True
        else:
            # 5 Sat, 6 Sun
            result = False
        return result

    def _download(self, source, url, file_extention):
        # If not have cache directory create one
        if not os.path.isdir(self.data_dir_name):
            import errno
            try:
                logger.info(f'Cache directory {self.data_dir_name} created successfully!')
                pathlib.Path(self.data_dir_name).mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    logger.error(f'Impossible to create cache directory {self.data_dir_name}')
                    raise
                pass
        # Setup file name
        today_date = datetime.now().strftime('%Y-%m-%d')
        file = f"{source}_{today_date}.pkl"
        cachefile_with_path = os.path.join(self.data_dir_name, file)
        cache_raw_file = f"{cachefile_with_path.replace('.pkl', '')}.{file_extention}"

        df = None
        try:
            f = open(cachefile_with_path, 'rb')
            df = joblib.load(cachefile_with_path)
            logger.info(f'Loaded {source} DATA from cache {cachefile_with_path}')
        except (IOError, OSError):
            if file_extention == 'html':
                logger.info(f'Downloading to {cache_raw_file} from {url}')
                r = requests.get(url)
                response = TextResponse(r.url, body=r.text, encoding="utf-8")
                logger.debug("Convert HTML to pandas dataframe")
                table = Table(response.xpath('(//table)[1]'))
                tullet = table.as_dicts()
                df = pd.DataFrame(tullet)
                # Cache
                joblib.dump(df, cachefile_with_path)
                logger.info(f'{source} data success cached at {cachefile_with_path}')
            else:
                logger.info(f'Downloading to {cache_raw_file} from {url}')
                r = requests.get(url, headers=self.headers, verify=False)
                logger.info(f'download size: {len(r.content)} ')
                with open(cache_raw_file, 'wb') as f:
                    f.write(r.content)

                if file_extention == 'csv':
                    logger.debug("Convert CSV to pandas dataframe")
                    df = pd.read_csv(cache_raw_file, sep='|')
                    logger.debug("Creating cache...")
                    joblib.dump(df, cachefile_with_path)
                    logger.info(f'{source} data success cached at {cachefile_with_path}')

                elif file_extention == 'xls':
                    logger.debug("Convert XLS to pandas dataframe")
                    df = pd.read_excel(cache_raw_file)
                    logger.debug("Creating cache...")
                    joblib.dump(df, cachefile_with_path)
                    logger.info(f'{source} data success cached at {cachefile_with_path}')

        return cachefile_with_path

    def download_tradition(self, date=None):
        tradition_date_format = None
        if self.is_yesterday_weekday():
            tradition_date_format = datetime.today() - timedelta(days=1)
        else:
            tradition_date_format = datetime.today() - timedelta(days=3)

        tradition_date_format = tradition_date_format.strftime('%Y%m%d')

        # https://www.traditionsef.com/dailyactivity/SEF16_MKTDATA_TFSU_20230123.csv
        URL_tradition = f"https://www.traditionsef.com/dailyactivity/SEF16_MKTDATA_TFSU_{tradition_date_format}.csv"

        file = self._download('TRADITION', URL_tradition, 'csv')

        return file

    def download_bgc(self, date=None):
        bgc_date_format = None
        if self.is_yesterday_weekday():
            bgc_date_format = datetime.today() - timedelta(days=1)
            bgc_date_format = bgc_date_format.strftime('%Y%m%d')
        else:
            bgc_date_format = datetime.today() - timedelta(days=3)
            bgc_date_format = bgc_date_format.strftime('%Y%m%d')

        # URL_BGC = f"http://dailyactprod.bgcsef.com/SEF/DailyAct/DailyAct_{bgc_date_format}-001.xls"
        URL_BGC = f"http://dailyactprod.bgcsef.com/SEF/DailyAct/DailyAct_{bgc_date_format}.xls"

        df = self._download('BGC', URL_BGC, 'xls')

    def download_prebontullet(self, date=None):
        URL_tulletprebon = "https://www.tullettprebon.com/swap-execution-facility/daily-activity-summary.aspx"

        df = self._download('TULLETPREBON', URL_tulletprebon, 'html')

        return df

    def download_gfi(self, date=None):
        date_format = '%Y-%m-%d'
        if date == None:
            gfi_date_format = None
            if (self.is_yesterday_weekday()):
                gfi_date_format = datetime.today() - timedelta(days=1)
            else:
                gfi_date_format = datetime.today() - timedelta(days=3)

            gfi_date_format = gfi_date_format.strftime(date_format)
        else:
            gfi_date_format = datetime.strptime(date, date_format).strftime('%Y-%m-%d')

        # "http://www.gfigroup.com/doc/sef/marketdata/2023-01-20_daily_trade_data.xls"
        URL_GFI = f"http://www.gfigroup.com/doc/sef/marketdata/{gfi_date_format}_daily_trade_data.xls"

        df = self._download('GFI', URL_GFI, 'xls')

        return df

    def download_all(self):
        self.download_bgc()
        self.download_tradition()
        self.download_prebontullet()
        self.download_gfi()

#
# # Press the green button in the gutter to run the script.
# if __name__ == '__main__':
#     logger.info("Starting service")
#
#     n = ndf()
#
#     n.download_all()
#
#     print(f'n.TRADITION_result\n {n.TRADITION_result}')
#
# # See PyCharm help at https://www.jetbrains.com/help/pycharm/
