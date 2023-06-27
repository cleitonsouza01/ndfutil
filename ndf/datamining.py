import re
from datetime import date, datetime, timedelta
from pathlib import Path

import joblib
import pandas
import matplotlib.pyplot as plt
from dateutil.relativedelta import relativedelta
from loguru import logger
from numerize import numerize

from ndf.util import get_BMF_date, get_cache_filename, get_last_bd
from ndf import download


class datamining:
    def __init__(self):
        self.year_size = 325
        self.data_dir_name = 'DATA'
        self.BGC_result = None
        self.TRADITION_result = None
        self.TULLETPREBON_result = None
        self.GFI_result = None

    def open_cache_file(self, source=None, date=None):
        if not source:
            logger.error('Please specify filename SOURCE to open')
            return
        if not date:
            ret = get_cache_filename(source)
        else:
            ret = get_cache_filename(source, date)

        return ret

    ##########################################
    # TRADITION CALCS
    def tradition_calcs(self, date=None, presentation=None):
        source = 'tradition'
        logger.info(f'{source} calcs starting')
        cache_filename = get_cache_filename(source) if not date else get_cache_filename(source, date)
        logger.debug(f'Opening CACHE FILE {cache_filename}')
        if not Path(cache_filename).is_file():
            logger.debug(f'File not found, trying to download {source} {date}')
            d = download.download()
            ret = d.download(source, date)
            if not ret:
                return None

        df_tradition = joblib.load(cache_filename)
        if df_tradition.empty:
            logger.info('TRADITION FILE EMPTY ***')
            return None
        df_tradition = df_tradition[
            ['Trade_Date', 'Internal_Prod_ID', 'Internal_Prod_Des', 'Contract_Type', 'Total_Notional_USD_DA',
             'Total_Trade_Count', ]].copy()
        df_tradition = df_tradition.rename(columns={'Internal_Prod_ID': 'Description'})
        df_tradition = df_tradition.rename(columns={'Total_Notional_USD_DA': 'Volume'})

        # TYPES CONVERTION
        df_tradition = df_tradition.astype({'Volume': 'int'})

        # ##print('df describe\n', df_tradition.describe())

        ##########################################
        # FILTERS FOR TRADITION
        #
        df_tradition = df_tradition[df_tradition.Description.str.contains(".*USDBRL_NDF.*")]
        df_tradition = df_tradition.reset_index(drop=True)

        df_tradition['Source'] = 'TRADITION'
        df_tradition['Class'] = None
        BMF1_DAYS = 9999
        BMF2_DAYS = 9999
        BMF3_DAYS = 9999

        # #print('before', df_tradition)

        # PTAX, TOMPTAX, BMF
        for index, row in df_tradition.iterrows():
            CLASS_STATUS = None
            description = row['Description']
            # #print(description)
            try:
                BRL = re.search(".*USDBRL_NDF.*", description)
            except:
                continue
            if BRL:
                # #print(row['Description'])

                DAYS = description.split('. | _')[0].split('D')[0]
                is_week = re.findall('.*W.*', DAYS)
                if is_week:
                    DAYS = DAYS.split('W')[0]
                    DAYS = int(DAYS) * 7
                else:
                    DAYS = int(DAYS) if DAYS.isnumeric() else 9999

                today = datetime.today()
                expire_date = today + timedelta(days=DAYS)
                BMF_DATE = get_BMF_date()
                delta_bmf = BMF_DATE - expire_date
                delta_bmf = int(delta_bmf.days)
                # #print(f'DAYS:{DAYS} | expire_date:{expire_date} | today:{today.day} | bmf:{BMF_DATE} | delta:{delta_bmf} ')

                PTAX = True if DAYS == 0 else False
                TOMPTAX = True if DAYS == 2 else False

                BMF = None
                BROKEN = None
                if 0 <= delta_bmf <= 3:
                    BMF = True
                else:
                    BROKEN = True
                # BMF2 ?
                # BMF3 ?

                # PTAX
                if PTAX:
                    CLASS_STATUS = "PTAX"
                # TOMPTAX
                elif TOMPTAX:
                    CLASS_STATUS = "TOMPTAX"
                    # BMF
                elif BMF:
                    CLASS_STATUS = "BMF"

                    # BROKEN
                elif BROKEN:
                    CLASS_STATUS = "BROKEN"

            if CLASS_STATUS:
                df_tradition.at[index, "Class"] = CLASS_STATUS

        volume_sum = df_tradition['Volume'].sum()
        df_tradition = df_tradition.append({'Volume': volume_sum, 'Class': 'TOTAL'}, ignore_index=True)

        #####################
        # NUMBERS FOR HUMAN

        # print(df_tradition.columns.tolist())
        # print(df_tradition)
        number_to_human = []
        for number in df_tradition['Volume']:
            converted = numerize.numerize(number)
            number_to_human.append(converted)

        df_tradition['Total for human'] = number_to_human

        #####################
        # NUMBERS FOR HUMAN

        df_tradition_summary = df_tradition.groupby('Class').sum()
        # print(df_tradition_summary)

        number_to_human = []
        for number in df_tradition.groupby('Class').sum()['Volume']:
            converted = numerize.numerize(number)
            number_to_human.append(converted)

        df_tradition_summary['Total for human'] = number_to_human

        #####################
        # DATAFRAME SORT
        df_tradition_summary = df_tradition_summary[['Total for human', 'Volume']]

        logger.debug(f'TRADITION Summary ===>\n{df_tradition_summary}\n')
        if presentation == 'raw':
            return df_tradition
        else:
            return df_tradition_summary

    ##########################################
    # TULLETPREBON CALCS
    def tulletprebon_calcs(self, date=None, presentation=None):
        source = 'tulletprebon'
        logger.info(f'{source} calcs starting')
        cache_filename = get_cache_filename(source) if not date else get_cache_filename(source, date)
        logger.debug(f'Opening CACHE FILE {cache_filename}')
        if not Path(cache_filename).is_file():
            logger.debug(f'File not found, trying to download {source} {date}')
            d = download.download()
            ret = d.download(source, date)
            if not ret:
                return None

        df = joblib.load(cache_filename) if Path(cache_filename).is_file() else pandas.DataFrame()
        if df.empty:
            logger.info('TULLET PREBON FILE EMPTY ***')
            return None
        YEAR = 330
        df['Source'] = 'TULLETPREBON'
        df['Class'] = ''
        df = df[df.Description.str.contains('.*BRL.*')]
        df = df[df.Description.str.contains('.*NDF.*')]
        df = df.reset_index(drop=True)

        logger.debug(f'tullet before mining:\n{df}')
        if df.empty:
            return None

        # TYPES CONVERTION
        logger.debug(df)
        df = df.astype({'Opening Price': 'float', 'Trade High': 'float',
                        'Trade Low': 'float', 'Closing Price': 'float',
                        'Num of Trades': 'int64', 'Total Notional Value': 'float'})

        #####################
        # DATAFRAME SORT
        # list(df.columns.values)
        df = df[['Batch Date', 'Asset Class', 'Class', 'Tradeable Instrument', 'Description',
                 'Opening Price', 'Trade High', 'Trade Low', 'Closing Price', 'Num of Trades',
                 'Total Notional Value', 'Source']]

        # %%
        #####################
        # FILTERS

        BMF1_DAYS = 9999
        BMF2_DAYS = 9999
        BMF3_DAYS = 9999

        for index, row in df.iterrows():
            # #print(row['Description'], end='')
            description = row['Description']
            BRL = re.search(".*BRL.*", description)
            NDF = re.search(".*NDF.*", description)
            if BRL and NDF:
                BMF1 = re.search(".*BMF1_NDF.*", description)
                BMF2 = re.search(".*BMF2_NDF.*", description)
                BMF3 = re.search(".*BMF3_NDF.*", description)
                CLASS_STATUS = ''

                DAYS = (description.split('. | _')[0].split('D')[0])
                # #print(f"{DAYS} {description.split('. | _')[0].split('D')}")
                DAYS = int(DAYS) if DAYS.isnumeric() else 0
                df.at[index, "Days"] = DAYS

                # BMF
                if BMF1:
                    CLASS_STATUS = "BMF1"
                    BMF1_DAYS = DAYS
                # BMF2 (2ND)
                elif BMF2:
                    CLASS_STATUS = "BMF2"
                    BMF2_DAYS = DAYS
                    # BMF3 (3RD)
                elif BMF3:
                    CLASS_STATUS = "BMF3"
                    BMF3_DAYS = DAYS

        for index, row in df.iterrows():
            # #print(row['Description'], end='')
            description = row['Description']
            BRL = re.search(".*BRL.*", description)
            NDF = re.search(".*NDF.*", description)
            if BRL and NDF:
                PTAX = re.search(".*TOD_NDF.*", description)
                TOMPTAX = re.search(".*TOM_NDF.*", description)
                BMF1 = re.search(".*BMF1_NDF.*", description)
                BMF2 = re.search(".*BMF2_NDF.*", description)
                BMF3 = re.search(".*BMF3_NDF.*", description)
                BROKEN = re.search(".*[0-9]D.*", description)
                MATURING = re.search(".Maturing.*", description)
                CLASS_STATUS = ''

                DAYS = (description.split('. | _')[0].split('D')[0])
                # #print(f"{DAYS} {description.split('. | _')[0].split('D')}")
                DAYS = int(DAYS) if DAYS.isnumeric() else 0
                df.at[index, "Days"] = DAYS if DAYS > 0 else 0

                # PTAX
                if PTAX:
                    CLASS_STATUS = "PTAX"
                # TOMPTAX
                elif TOMPTAX:
                    CLASS_STATUS = "TOMPTAX"
                    # BMF
                elif BMF1:
                    CLASS_STATUS = "BMF1"
                    # BMF2 (2ND)
                elif BMF2:
                    CLASS_STATUS = "BMF2"
                    # BMF3 (3RD)
                elif BMF3:
                    CLASS_STATUS = "BMF3"
                # BROKEN
                elif BROKEN:
                    CLASS_STATUS = "BROKEN"

                    DESC = ''
                    # LONGER
                    if DAYS > 30 and DAYS < YEAR:
                        CLASS_STATUS = "LONGER"
                        # >= 1 YR
                    elif DAYS >= YEAR:
                        CLASS_STATUS = "GT 1 YEAR"
                        # MATURING (broken)
                elif MATURING:
                    CLASS_STATUS = "XP"
                    MATURING_DATE = re.findall("\d{4}-\d{2}-\d{2}", description)
                    MATURING_DATE = MATURING_DATE[0]

                    # date calc
                    date_format = "%Y-%m-%d"
                    # a = datetime.strptime(MATURING_DATE, date_format)
                    # b = datetime.strptime(batch_date, date_format)
                    # MATURING_DAYS = a - b

                    # #print(f"Days to maturing")
            # #print_color(CLASS_STATUS)
            df.at[index, "Class"] = CLASS_STATUS

            if CLASS_STATUS == 'BROKEN' and DAYS == 0:
                df.at[index, "Class"] = 'PTAX'
            elif CLASS_STATUS == 'BROKEN' and DAYS == BMF1_DAYS:
                df.at[index, "Class"] = 'BMF1'
            elif CLASS_STATUS == 'BROKEN' and DAYS == BMF2_DAYS:
                df.at[index, "Class"] = 'BMF2'
            elif CLASS_STATUS == 'BROKEN' and DAYS == BMF3_DAYS:
                df.at[index, "Class"] = 'BMF2'
            if CLASS_STATUS == 'BROKEN':
                pass
                # print(f'DAYS:{DAYS} BMF1:{BMF1_DAYS}\nDAYS:{DAYS} BMF2:{BMF2_DAYS}\nDAYS:{DAYS} BMF3:{BMF3_DAYS}\n')

        volume_sum = df['Total Notional Value'].sum()
        df = df.append({'Total Notional Value': volume_sum, 'Class': 'TOTAL'}, ignore_index=True)

        #####################
        # NUMBERS FOR HUMAN

        number_to_human = []
        for number in df['Total Notional Value']:
            converted = numerize.numerize(number)
            number_to_human.append(converted)

        df['Total for human'] = number_to_human

        df_summary = df.groupby('Class').sum()
        # df.drop(columns=['Num of Trades', 'Days'], inplace=True)

        number_to_human = []
        for number in df.groupby('Class').sum()['Total Notional Value']:
            converted = numerize.numerize(number)
            number_to_human.append(converted)

        df_summary['Total for human'] = number_to_human

        #####################
        # DATAFRAME SORT
        df_summary.drop(columns=["Opening Price", "Trade Low", "Trade High",
                                 "Closing Price", 'Num of Trades', 'Days'], inplace=True)
        # list(df.columns.values)
        df_summary = df_summary[['Total for human', 'Total Notional Value']]
        df_summary.rename(columns={'Total Notional Value': 'Volume'}, inplace=True)

        logger.debug(f'TULLETPREBON Summary ===>\n{df_summary}\n')
        if presentation == 'raw':
            return df
        else:
            return df_summary

    ##########################################
    # GFI CALCS
    def gfi_calcs(self, date=None, presentation=None):
        source = 'gfi'
        logger.info(f'{source} calcs starting')
        cache_filename = get_cache_filename(source) if not date else get_cache_filename(source, date)
        logger.debug(f'Opening CACHE FILE {cache_filename}')
        if not Path(cache_filename).is_file():
            logger.debug(f'File not found, trying to download {source} {date}')
            d = download.download()
            ret = d.download(source, date)
            if not ret:
                return None

        df = joblib.load(cache_filename)
        if df.empty:
            logger.info('GFI FILE EMPTY ***')
            return None
        df.drop('Unnamed: 0', axis=1, inplace=True)
        df.drop([0, 1, 2], axis=0, inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.columns = df.iloc[0]
        df.reset_index(drop=True, inplace=True)
        df = df.rename(columns={'Contract Description': 'Description'})
        df.drop(['Asset Class', 'Open', 'Low', 'High', 'Close', 'Block', 'Currency'], axis=1, inplace=True)
        df = df[df['Volume'].notna()]

        # df.drop([0], axis=0, inplace=True)
        # df = df.rename(columns={'Instrument Description': 'Description'})

        # TYPES CONVERTION
        # df = df.astype({'Volume':'float'})
        # %%
        ##########################################
        # FILTERS FOR GFI
        #
        df = df[df.Description.str.contains(".*USDBRL NDF.*")]
        df = df.reset_index(drop=True)

        logger.debug(f'GFI before mining:\n{df}')
        if df.empty:
            return None

        df['Source'] = 'GFI'
        df['Class'] = None
        BMF1_DAYS = 9999
        BMF2_DAYS = 9999
        BMF3_DAYS = 9999

        # PTAX, TOMPTAX, BMF
        for index, row in df.iterrows():
            CLASS_STATUS = None
            description = row['Description']
            # #print(description)
            try:
                BRL = re.search(".*USDBRL NDF.*", description)
            except:
                continue
            if BRL:
                # print(row['Description'])
                row_date = description.split(' ')[2]
                row_date = datetime.strptime(row_date, "%d%b%Y")
                today = datetime.today()
                # BMF
                if row_date == get_last_bd(row_date):
                    # BMF 1
                    # print(f'{row_date.month} == {today.month}  |  {row_date} | {description}')
                    if row_date.month == today.month:
                        CLASS_STATUS = "BMF"
                    elif row_date.month == today.month + 1:
                        CLASS_STATUS = "BMF2"
                    elif row_date.month == today.month + 2:
                        CLASS_STATUS = "BMF3"
                    elif row_date.month == today.month + 3:
                        CLASS_STATUS = "BMF4"
                    else:
                        bmf = get_BMF_date(row_date)
                        CLASS_STATUS = f"BMF {bmf.month}/{bmf.year}"
            if CLASS_STATUS:
                df.at[index, "Class"] = CLASS_STATUS

        # Resto BROKEN
        for index, row in df.iterrows():
            description = row['Description']
            BRL = re.search(".*BRL NDF.*", description)
            if BRL:
                if not df.at[index, "Class"]:
                    CLASS_STATUS = "BROKEN"
                    df.at[index, "Class"] = CLASS_STATUS

        volume_sum = df['Volume'].sum()
        df = df.append({'Volume': volume_sum, 'Class': 'TOTAL'}, ignore_index=True)

        #####################
        # NUMBERS FOR HUMAN
        df_summary = df.groupby('Class').sum()

        number_to_human = []
        for number in df.groupby('Class').sum()['Volume']:
            converted = numerize.numerize(number)
            number_to_human.append(converted)

        df_summary['Total for human'] = number_to_human

        #####################
        # DATAFRAME SORT
        df_summary = df_summary[['Total for human', 'Volume']]

        logger.debug(f'GFI _summary ===>\n{df_summary}\n')
        if presentation == 'raw':
            return df
        else:
            return df_summary

    ##########################################
    # BGC CALCS
    def bgc_calcs(self, date=None, presentation=None):
        source = 'bgc'
        logger.info(f'{source} calcs starting')
        cache_filename = get_cache_filename(source) if not date else get_cache_filename(source, date)
        logger.debug(f'Opening CACHE FILE {cache_filename}')
        if not Path(cache_filename).is_file():
            logger.debug(f'File not found, trying to download {source} {date}')
            d = download.download()
            ret = d.download(source, date)
            if not ret:
                return None

        df = joblib.load(cache_filename)
        if df.empty:
            logger.info('BGC FILE EMPTY ***')
            return None
        df.drop('Unnamed: 0', axis=1, inplace=True)
        df.drop([0, 1, 2], axis=0, inplace=True)
        df.reset_index(drop=True, inplace=True)
        df.columns = df.iloc[0]
        df.reset_index(drop=True, inplace=True)
        df = df.rename(columns={'Contract Description': 'Description'})
        df.drop(['Asset Class', 'Open', 'Low', 'High', 'Close', 'Block', 'Currency'], axis=1, inplace=True)
        df = df[df['Volume'].notna()]

        # df.drop([0], axis=0, inplace=True)
        # print(df.columns)
        df = df.rename(columns={'InstrumentDescription': 'Description'})
        # print(df.columns)

        # TYPES CONVERTION
        # df = df.astype({'Volume':'float'})
        # %%
        ##########################################
        # FILTERS FOR BGC
        #
        df = df[df.Description.str.contains(".*USDBRL NDF.*")]
        df = df.reset_index(drop=True)

        logger.debug(f'BGC before mining:\n{df}')
        if df.empty:
            return None

        df['Source'] = 'BGC'
        df['Class'] = None
        BMF1_DAYS = 9999
        BMF2_DAYS = 9999
        BMF3_DAYS = 9999

        # PTAX, TOMPTAX, BMF
        for index, row in df.iterrows():
            CLASS_STATUS = None
            description = row['Description']
            # #print(description)
            try:
                BRL = re.search(".*USDBRL NDF.*", description)
            except:
                continue
            if BRL:
                # print(row['Description'])
                row_date = description.split(' ')[2]
                row_date = datetime.strptime(row_date, "%d%b%Y")
                today = datetime.today()
                # BMF
                if row_date == get_last_bd(row_date):
                    # BMF 1
                    # print(f'{row_date.month} == {today.month}  |  {row_date} | {description}')
                    if row_date.month == today.month:
                        CLASS_STATUS = "BMF"
                    elif row_date.month == today.month + 1:
                        CLASS_STATUS = "BMF2"
                    elif row_date.month == today.month + 2:
                        CLASS_STATUS = "BMF3"
                    elif row_date.month == today.month + 3:
                        CLASS_STATUS = "BMF4"
                    else:
                        bmf = get_BMF_date(row_date)
                        CLASS_STATUS = f"BMF {bmf.month}/{bmf.year}"
            if CLASS_STATUS:
                df.at[index, "Class"] = CLASS_STATUS

        # Resto BROKEN
        for index, row in df.iterrows():
            description = row['Description']
            BRL = re.search(".*BRL NDF.*", description)
            if BRL:
                if not df.at[index, "Class"]:
                    CLASS_STATUS = "BROKEN"
                    df.at[index, "Class"] = CLASS_STATUS

        volume_sum = df['Volume'].sum()
        df = df.append({'Volume': volume_sum, 'Class': 'TOTAL'}, ignore_index=True)

        #####################
        # NUMBERS FOR HUMAN
        df_summary = df.groupby('Class').sum()

        number_to_human = []
        for number in df.groupby('Class').sum()['Volume']:
            converted = numerize.numerize(number)
            number_to_human.append(converted)

        df_summary['Total for human'] = number_to_human

        #####################
        # DATAFRAME SORT
        df_summary = df_summary[['Total for human', 'Volume']]

        logger.debug(f'BGC _summary ===>\n{df_summary}\n')
        if presentation == 'raw':
            return df
        else:
            return df_summary

    ##########################################
    # MARKET SUMMARY
    def market_summary(self):
        df = pandas.DataFrame({'Class': ['TOTAL'], 'Total for human': [0], 'Volume': [0]}).set_index('Class')
        tulletprebon = self.tulletprebon_calcs() if isinstance(self.tulletprebon_calcs(), pandas.DataFrame) else df
        tradition = self.tradition_calcs() if isinstance(self.tradition_calcs(), pandas.DataFrame) else df
        gfi = self.gfi_calcs() if isinstance(self.gfi_calcs(), pandas.DataFrame) else df
        bgc = self.bgc_calcs() if isinstance(self.bgc_calcs(), pandas.DataFrame) else df

        tulletprebon['source'] = 'TulletPrebon'
        tradition['source'] = 'Tradition'
        bgc['source'] = 'bgc'
        gfi['source'] = 'gfi'

        df_result = pandas.concat([tulletprebon, tradition, bgc, gfi])
        df_totals = df_result.query('Class == "TOTAL"')

        return df_totals

    ##########################################
    # TOTAL
    def market_total(self):
        df_total = self.market_summary()
        market_total = df_total.sum()['Volume']
        market_total = numerize.numerize(float(market_total))
        return market_total

    def generate_chart(self):
        market_total = self.market_total()
        summary = self.market_summary()

        # Labels prepare
        labels = summary[['source', 'Total for human']]
        labels = labels.values.tolist()
        labels_chart = []
        for item in labels:
            labels_chart.append(f'{item[0]} ${item[1]}')

        font1 = {'family': 'serif', 'color': 'blue', 'size': 18}
        colors = ['#045ca3', '#f5b12b', '#d11e20', '#8ec5f2']
        fig = plt.figure(figsize=(5, 6))
        plt.pie(summary['Volume'], labels=labels_chart, labeldistance=1.15, autopct='%1.0f%%',
                wedgeprops={'linewidth': 3, 'edgecolor': 'white'}, colors=colors,
                textprops={'fontsize': 14})
        plt.legend(labels_chart, loc='lower left', bbox_to_anchor=(-0.35, .5), fontsize=10)
        plt.title(f'NDF Market Summary\n Total ${market_total}', fontdict=font1)
        plt.savefig('pie.png', dpi=fig.dpi, bbox_inches='tight')

        return True
