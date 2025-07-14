# NDF download manager
import pathlib
import time
import random
from datetime import date, datetime, timedelta
import os
import sys

import pandas
import pandas as pd
import joblib
import requests as requests
from loguru import logger
import urllib3
import concurrent.futures

# Optional: cloudscraper for Cloudflare bypass
try:
    import cloudscraper
    CLOUDSCRAPER_AVAILABLE = True
except ImportError:
    CLOUDSCRAPER_AVAILABLE = False
    logger.warning("cloudscraper not available. Install with: pip install cloudscraper")

import ndf.util
from ndf.table import Table
from scrapy.http import TextResponse

from ndf.util import is_weekday, is_yesterday_weekday, is_holiday, is_weekend

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class DownloadFailed(Exception):
    def __init__(self, source, url, reason):
        super().__init__(f"Download failed for {source} from {url}: {reason}")
        self.source = source
        self.url = url
        self.reason = reason

class download:
    """
    Download manager for NDF data sources.

    For each data source, you can override the download URL in two ways:
    1. Provide a static URL (for sources that do not require a date in the URL).
    2. Provide a base URL, a suffix, and a date format. The class will build the URL automatically.

    Example usage:
        # Use a static URL for TullettPrebon, and custom dynamic URL for BGC
        d = download(
            tullettprebon_url="https://my.custom.server/staticfile.html",
            bgc_url_base="http://my.custom.server/BGC_",
            bgc_url_suffix=".xlsx",
            bgc_date_format="%Y%m%d"
        )
    """
    def __init__(self,
                 # BGC
                 bgc_url=None,
                 bgc_url_base="http://dailyactprod.bgcsef.com/SEF/DailyAct/DailyAct_",
                 bgc_url_suffix=".xlsx",
                 bgc_date_format="%Y%m%d",
                 # Tradition
                 tradition_url=None,
                 tradition_url_base="https://www.traditionsef.com/dailyactivity/SEF16_MKTDATA_TFSU_",
                 tradition_url_suffix=".csv",
                 tradition_date_format="%Y%m%d",
                 # TullettPrebon
                 tullettprebon_url="https://www.tullettprebon.com/swap-execution-facility/daily-activity-summary.aspx",
                 # GFI
                 gfi_url=None,
                 gfi_url_base="https://www.gfigroup.com/doc/sef/marketdata/",
                 gfi_url_suffix="_daily_trade_data.xlsx",
                 gfi_date_format="%Y-%m-%d"
                 ):
        """
        Args:
            For each source, you can specify either:
                - A static URL (e.g., tullettprebon_url)
                - Or, a base URL, suffix, and date format (e.g., bgc_url_base, bgc_url_suffix, bgc_date_format)
        """
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
        
        # Create a session for persistent connections and cookies
        self.session = requests.Session()
        
        # Create cloudscraper session if available
        if CLOUDSCRAPER_AVAILABLE:
            self.cloudscraper_session = cloudscraper.create_scraper(
                browser={
                    'browser': 'chrome',
                    'platform': 'darwin',
                    'desktop': True
                }
            )
        else:
            self.cloudscraper_session = None
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"macOS"',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        
        # Update session headers
        self.session.headers.update(self.headers)
        
        # BGC
        self.bgc_url = bgc_url
        self.bgc_url_base = bgc_url_base
        self.bgc_url_suffix = bgc_url_suffix
        self.bgc_date_format = bgc_date_format
        # Tradition
        self.tradition_url = tradition_url
        self.tradition_url_base = tradition_url_base
        self.tradition_url_suffix = tradition_url_suffix
        self.tradition_date_format = tradition_date_format
        # TullettPrebon
        self.tullettprebon_url = tullettprebon_url
        # GFI
        self.gfi_url = gfi_url
        self.gfi_url_base = gfi_url_base
        self.gfi_url_suffix = gfi_url_suffix
        self.gfi_date_format = gfi_date_format

    def download_manager(self, url):
        r = None
        last_error = None
        logger.info(f'Download settings: MAX_RETRIES:{self.MAX_RETRIES} | TIME_TO_WAIT:{self.TIME_TO_WAIT}')
        for attempt in range(self.MAX_RETRIES):
            # Add random delay to look more human-like
            if attempt > 0:
                human_delay = random.uniform(1, 3)
                logger.info(f'Adding human-like delay: {human_delay:.2f} seconds')
                time.sleep(human_delay)
            
            logger.info(f'Download attempt {attempt + 1} trying {url}')
            try:
                r = self.session.get(url, verify=False)
                status_code = r.status_code if r else None
                logger.info(f"HTTP status: {status_code}")
                if r is not None:
                    logger.info(f"Final URL after redirects: {r.url}")
                
                if r and r.status_code == 200:
                    logger.info('Download Ok')
                    return r, None
                elif r and (r.status_code == 403 or "Just a moment..." in r.text):
                    # Try advanced Cloudflare bypass
                    logger.info('Detected Cloudflare challenge, trying advanced bypass...')
                    r_bypass, bypass_error = self._handle_cloudflare_challenge(url)
                    if r_bypass:
                        logger.info('Advanced Cloudflare bypass successful!')
                        return r_bypass, None
                    else:
                        logger.error(f'Advanced Cloudflare bypass failed: {bypass_error}')
                else:
                    # Debug: why didn't we trigger Cloudflare bypass?
                    if r:
                        logger.debug(f"Debug: r exists, status={r.status_code}, has_js_challenge={'Just a moment...' in r.text}")
                    else:
                        logger.debug("Debug: r is None")
                if r is not None:
                    logger.error(f"Download failed: HTTP {r.status_code}")
                    logger.error(f"Response headers: {r.headers}")
                    logger.error(f"Response content (first 500 chars): {r.text[:500]}")
                last_error = f"HTTP {r.status_code}" if r else "No response"
            except requests.exceptions.RequestException as e:
                last_error = str(e)
                logger.error(f'Download error: {e}')
            wait = self.TIME_TO_WAIT * (attempt + 1) if attempt < 3 else self.TIME_TO_WAIT * (attempt * 3)
            logger.info(f'Download fail, retry in {wait} seconds...')
            time.sleep(wait)
        logger.error(f"All download attempts failed: {last_error}")
        return None, last_error

    def _download(self, source, url, file_extention, date=None):
        logger.info(f'{source.upper()} - Download start >>>')
        source = source.lower()
        # If not have cache directory create one
        logger.debug(f'Try download {source} from {url}')

        # Check if DATA directory exist
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
            df = joblib.load(cachefile_with_path)
            logger.info(f'Loaded {source} DATA from cache {cachefile_with_path}')
            return df, None  # Tuple: (data, error)
        else:
            if file_extention == 'html':
                logger.info(f'Downloading to {cache_raw_file} from {url}')
                r, error = self.download_manager(url)
                if r:
                    logger.info(f'{source.upper()} - Download OK')
                    response = TextResponse(r.url, body=r.text, encoding="utf-8")
                    logger.debug("Convert HTML to pandas dataframe")
                    table = Table(response.xpath('(//table)[1]'))
                    tullet = table.as_dicts()
                    df = pd.DataFrame(tullet)
                    # Cache
                    joblib.dump(df, cachefile_with_path)
                    logger.info(f'{source} data success cached at {cachefile_with_path}')
                    return df, None
                else:
                    logger.error(f"Download failed for {source}: {error}")
                    # Option A: return None, error
                    return None, error
                    # Option B: raise DownloadFailed(source, url, error)
            else:
                logger.info(f'Downloading to {cache_raw_file} from {url}')
                r, error = self.download_manager(url)
                if r:
                    logger.info(f'{source.upper()} - Download OK')
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
                            return df, None

                        elif file_extention == 'xls' or file_extention == 'xlsx':
                            logger.debug(f"Convert {file_extention.upper()} to pandas dataframe")
                            df = pd.read_excel(cache_raw_file)
                            logger.debug("Creating cache...")
                            joblib.dump(df, cachefile_with_path)
                            logger.info(f'{source} data success cached at {cachefile_with_path}')
                            return df, None
                    else:
                        logger.info("File size too small, ignoring and returning false")
                        error = "Downloaded file size too small"
                        return None, error
                        # Option B: raise DownloadFailed(source, url, error)
                else:
                    logger.error(f"Download failed for {source}: {error}")
                    return None, error
                    # Option B: raise DownloadFailed(source, url, error)
        # Should not reach here
        return None, "Unknown error"

    def download(self, source, date=None):
        if source == "tradition":
            return self.download_tradition(date)
        elif source == "bgc":
            return self.download_bgc(date)
        elif source == "tulletprebon":
            return self.download_prebontullet(date)
        elif source == "gfi":
            return self.download_gfi(date)

    def _build_url(self, static_url, base, suffix, date_str):
        """
        Helper to build a URL from components, or use a static URL if provided.
        """
        if static_url:
            return static_url
        return f"{base}{date_str}{suffix}"
    
    def _handle_cloudflare_challenge(self, url):
        """
        Advanced Cloudflare bypass using multiple techniques
        """
        logger.info('Attempting advanced Cloudflare bypass...')
        
        # Method 1: Try with cloudscraper with different configurations
        if CLOUDSCRAPER_AVAILABLE:
            configs = [
                {'browser': 'chrome', 'platform': 'darwin', 'desktop': True},
                {'browser': 'chrome', 'platform': 'windows', 'desktop': True},
                {'browser': 'firefox', 'platform': 'linux', 'desktop': True},
            ]
            
            for config in configs:
                try:
                    logger.info(f'Trying cloudscraper with config: {config}')
                    scraper = cloudscraper.create_scraper(browser=config)
                    
                    # Try multiple times with delays for JS challenges
                    for attempt in range(3):
                        r = scraper.get(url, verify=False, timeout=30)
                        if r and r.status_code == 200 and "Just a moment..." not in r.text:
                            logger.info(f'Success with config: {config} on attempt {attempt + 1}')
                            return r, None
                        elif r and "Just a moment..." in r.text:
                            logger.info(f'Got JS challenge, waiting {5 * (attempt + 1)} seconds...')
                            time.sleep(5 * (attempt + 1))
                        else:
                            logger.warning(f'Config {config} attempt {attempt + 1} failed with status: {r.status_code if r else "No response"}')
                            
                except Exception as e:
                    logger.error(f'Config {config} error: {e}')
                    
                # Wait between configurations
                time.sleep(random.uniform(3, 7))
        
        # Method 2: Try with different user agents manually
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
        
        for ua in user_agents:
            try:
                logger.info(f'Trying with User-Agent: {ua[:50]}...')
                session = requests.Session()
                session.headers.update({
                    'User-Agent': ua,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                })
                
                # Try multiple times for JS challenges
                for attempt in range(2):
                    r = session.get(url, verify=False, timeout=30)
                    if r and r.status_code == 200 and "Just a moment..." not in r.text:
                        logger.info(f'Success with User-Agent: {ua[:50]}... on attempt {attempt + 1}')
                        return r, None
                    elif r and "Just a moment..." in r.text:
                        logger.info(f'Got JS challenge with UA, waiting {3 * (attempt + 1)} seconds...')
                        time.sleep(3 * (attempt + 1))
                    else:
                        logger.warning(f'User-Agent attempt {attempt + 1} failed with status: {r.status_code if r else "No response"}')
                        
            except Exception as e:
                logger.error(f'User-Agent {ua[:50]}... error: {e}')
                
            # Wait between user agents
            time.sleep(random.uniform(2, 4))
        
        return None, "All Cloudflare bypass methods failed"

    def download_tradition(self, date=None):
        date_format = '%Y-%m-%d'
        if date:
            dt = datetime.strptime(date, date_format)
        else:
            if is_yesterday_weekday():
                dt = datetime.today() - timedelta(days=1)
            else:
                dt = datetime.today() - timedelta(days=3)
        date_str = dt.strftime(self.tradition_date_format)
        URL_tradition = self._build_url(self.tradition_url, self.tradition_url_base, self.tradition_url_suffix, date_str)
        df, error = self._download('TRADITION', URL_tradition, 'csv', date)
        if error:
            return False, error
        return True, None

    def download_bgc(self, date=None):
        date_format = '%Y-%m-%d'
        if date:
            dt = datetime.strptime(date, date_format) - timedelta(days=1)
        else:
            if is_yesterday_weekday():
                dt = datetime.today() - timedelta(days=1)
            else:
                dt = datetime.today() - timedelta(days=3)
        date_str = dt.strftime(self.bgc_date_format)
        URL_BGC = self._build_url(self.bgc_url, self.bgc_url_base, self.bgc_url_suffix, date_str)
        df, error = self._download('BGC', URL_BGC, 'xlsx', date)
        if error:
            return False, error
        return True, None

    def download_prebontullet(self, date=None):
        if date:
            return None, "Date not supported for TULLETPREBON"
        URL_tulletprebon = self.tullettprebon_url
        df, error = self._download('TULLETPREBON', URL_tulletprebon, 'html')
        if error:
            return False, error
        return True, None

    def download_gfi(self, date=None):
        date_format = '%Y-%m-%d'
        if date:
            dt = datetime.strptime(date, date_format)
        else:
            if is_yesterday_weekday():
                dt = datetime.today() - timedelta(days=1)
            else:
                dt = datetime.today() - timedelta(days=3)
        date_str = dt.strftime(self.gfi_date_format)
        URL_GFI = self._build_url(self.gfi_url, self.gfi_url_base, self.gfi_url_suffix, date_str)
        df, error = self._download('GFI', URL_GFI, 'xls', date)
        if error:
            return False, error
        return True, None

    def download_all(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_to_source = {
                executor.submit(self.download_bgc): 'bgc',
                executor.submit(self.download_tradition): 'tradition',
                executor.submit(self.download_prebontullet): 'prebontullet',
                executor.submit(self.download_gfi): 'gfi'
            }
            results = {}
            for future in concurrent.futures.as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    result, error = future.result()
                    results[source] = (result, error)
                except Exception as exc:
                    results[source] = (False, str(exc))

        print('bgc ok') if results['bgc'][0] else print(f'bgc erro: {results["bgc"][1]}')
        print('tradition ok') if results['tradition'][0] else print(f'tradition erro: {results["tradition"][1]}')
        print('prebontullet ok') if results['prebontullet'][0] else print(f'prebontullet erro: {results["prebontullet"][1]}')
        print('gfi ok') if results['gfi'][0] else print(f'gfi erro: {results["gfi"][1]}')
