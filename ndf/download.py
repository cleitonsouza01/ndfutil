# NDF download manager
import pathlib
import time
from datetime import date, datetime, timedelta
import os
import sys

import pandas
import pandas as pd
import joblib
import requests as requests
from loguru import logger

import ndf.util
from ndf.table import Table
from scrapy.http import TextResponse

from ndf.util import is_weekday, is_yesterday_weekday, is_holiday, is_weekend


class download:
    def __init__(self):
        self.year_size = 325
        self.data_dir_name = 'DATA'
        self.BGC_result = None
        self.TRADITION_result = None
        self.TULLETPREBON_result = None
        self.GFI_result = None
        self.MAX_RETRIES = 6
        self.TIME_TO_WAIT = 10
        self.FILE_SIZE_MIN = 2000
        self.df_empty = pandas.DataFrame({'Class': ['TOTAL'], 'Total for human': [0], 'Volume': [0]}).set_index('Class')
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/50.0.2661.102 Safari/537.36'}

    def download_manager(self, url):
        r = None
        logger.info(f'Download settings: MAX_RETRIES:{self.MAX_RETRIES} | TIME_TO_WAIT:{self.TIME_TO_WAIT}')
        for _ in range(self.MAX_RETRIES):
            r = requests.get(url, headers=self.headers, verify=False)
            if r.ok:
                logger.info(f'Download Ok')
                break
            else:
                wait = self.TIME_TO_WAIT * (_ + 1) if _ < 3 else self.TIME_TO_WAIT * (_ * 3)
                logger.info(f'Download fail, retry in {wait} seconds...')
                time.sleep(wait)
        return r

    def _download(self, source, url, file_extention, date=None):
        logger.info(f'{source.upper()} - Download start >>>')
        source = source.lower()
        # If not have cache directory create one
        logger.debug(f'Try download {source} from {url}')
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
        cachefile_with_path = ndf.util.get_cache_filename(source, date)
        cache_raw_file = f"{cachefile_with_path.replace('.pkl', '')}.{file_extention}"

        df = None

        # Load cache if exist
        if pathlib.Path(cachefile_with_path).is_file():
            f = open(cachefile_with_path, 'rb')
            df = joblib.load(cachefile_with_path)
            logger.info(f'Loaded {source} DATA from cache {cachefile_with_path}')
        else:
            if file_extention == 'html':
                logger.info(f'Downloading to {cache_raw_file} from {url}')
                r = self.download_manager(url)
                if r:
                    response = TextResponse(r.url, body=r.text, encoding="utf-8")
                    logger.debug("Convert HTML to pandas dataframe")
                    table = Table(response.xpath('(//table)[1]'))
                    tullet = table.as_dicts()
                    df = pd.DataFrame(tullet)
                    # Cache
                    joblib.dump(df, cachefile_with_path)
                    logger.info(f'{source} data success cached at {cachefile_with_path}')
                else:
                    df = self.df_empty
            else:
                logger.info(f'Downloading to {cache_raw_file} from {url}')
                r = self.download_manager(url)
                if r:
                    file_size = len(r.content)
                    logger.info(f'download size: {file_size} ')
                    if file_size > self.FILE_SIZE_MIN:
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
                    else:
                        logger.info("File size too small, ignoring and returning false")
                        df = self.df_empty
                else:
                    logger.info("Download failed")
                    df = self.df_empty
        return df

    def download(self, source, date=None):
        if source == "tradition":
            return self.download_tradition(date)
        elif source == "bgc":
            return self.download_bgc(date)
        elif source == "tulletprebon":
            return self.download_prebontullet(date)
        elif source == "gfi":
            return self.download_gfi(date)

    def download_tradition(self, date=None):
        tradition_date_format = None
        date_format = '%Y-%m-%d'
        if date:
            tradition_date_format = datetime.strptime(date, date_format)
        else:
            if is_yesterday_weekday():
                tradition_date_format = datetime.today() - timedelta(days=1)
            else:
                tradition_date_format = datetime.today() - timedelta(days=3)

        tradition_date_format = tradition_date_format.strftime('%Y%m%d')

        # https://www.traditionsef.com/dailyactivity/SEF16_MKTDATA_TFSU_20230123.csv
        URL_tradition = f"https://www.traditionsef.com/dailyactivity/SEF16_MKTDATA_TFSU_{tradition_date_format}.csv"

        df = self._download('TRADITION', URL_tradition, 'csv', date)

        if isinstance(df, pd.DataFrame):
            return True
        else:
            return False

    def download_bgc(self, date=None):
        bgc_date_format = None
        date_format = '%Y-%m-%d'
        if date:
            bgc_date_format = datetime.strptime(date, date_format).strftime('%Y%m%d')
        else:
            if is_yesterday_weekday():
                bgc_date_format = datetime.today() - timedelta(days=1)
                bgc_date_format = bgc_date_format.strftime('%Y%m%d')
            else:
                bgc_date_format = datetime.today() - timedelta(days=3)
                bgc_date_format = bgc_date_format.strftime('%Y%m%d')

        # URL_BGC = f"http://dailyactprod.bgcsef.com/SEF/DailyAct/DailyAct_{bgc_date_format}-001.xls"
        URL_BGC = f"http://dailyactprod.bgcsef.com/SEF/DailyAct/DailyAct_{bgc_date_format}.xls"

        df = self._download('BGC', URL_BGC, 'xls', date)

        if isinstance(df, pd.DataFrame):
            return True
        else:
            return False

    def download_prebontullet(self, date=None):
        if date:
            return None

        URL_tulletprebon = "https://www.tullettprebon.com/swap-execution-facility/daily-activity-summary.aspx"

        df = self._download('TULLETPREBON', URL_tulletprebon, 'html')

        if isinstance(df, pd.DataFrame):
            return True
        else:
            return False

    def download_gfi(self, date=None):
        date_format = '%Y-%m-%d'
        gfi_date_format = None
        if date:
            gfi_date_format = datetime.strptime(date, date_format).strftime('%Y-%m-%d')
        else:
            if (is_yesterday_weekday()):
                gfi_date_format = datetime.today() - timedelta(days=1)
            else:
                gfi_date_format = datetime.today() - timedelta(days=3)

            gfi_date_format = gfi_date_format.strftime(date_format)

        # "http://www.gfigroup.com/doc/sef/marketdata/2023-01-20_daily_trade_data.xls"
        URL_GFI = f"http://www.gfigroup.com/doc/sef/marketdata/{gfi_date_format}_daily_trade_data.xls"

        df = self._download('GFI', URL_GFI, 'xls', date)

        if isinstance(df, pd.DataFrame):
            return True
        else:
            return False

    def download_all(self):
        bgc = self.download_bgc()
        tradition = self.download_tradition()
        prebontullet = self.download_prebontullet()
        gfi = self.download_gfi()
        print('bgc ok') if bgc else print('bgc erro')
        print('tradition ok') if tradition else print('tradition erro')
        print('prebontullet ok') if prebontullet else print('prebontullet erro')
        print('gfi ok') if gfi else print('gfi erro')
