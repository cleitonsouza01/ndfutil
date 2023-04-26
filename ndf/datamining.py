import re
from datetime import date, datetime, timedelta
import holidays
import joblib
from loguru import logger
from numerize import numerize

import pandas as pd


def is_weekday(date):
    weekno = date.weekday()
    result = None
    if weekno < 5:
        result = True
    else:
        # 5 Sat, 6 Sun
        result = False
    return result


def is_holiday(date):
    us_holidays = holidays.NYSE()
    br_holidays = holidays.BR()
    if (date.date() in us_holidays) or (date.date() in br_holidays):
        return True
    else:
        return False


#####################
# GET BMF DATE

def get_BMF_date(month=None, year=None):
    BMF_date = datetime.today()
    BMF_date = (BMF_date.replace(day=1) + timedelta(days=32)).replace(day=1)

    if month:
        BMF_date = BMF_date.replace(month=month)

    if year:
        BMF_date = BMF_date.replace(year=year)
    # 1st BD: dt + BusinessDay()
    # 2nd BD: dt + 2 * BusinessDay()
    # 3rd BD: dt + 3 * BusinessDay()

    # print(f'BMF calculated {BMF_date.date()}')
    # BMF_date = BMF_date + 2 * BusinessDay()
    # BMF_date = BMF_date + timedelta(days=1)
    count = 1
    while (not is_weekday(BMF_date) or is_holiday(BMF_date)) or count < 2:
        BMF_date = BMF_date + timedelta(days=1)
        count += 1
    # print(f'BMF after weekday {BMF_date.date()}')

    # while is_holiday(BMF_date):
    #     print(f"{BMF_date.date()} is holiday")
    #     #BMF_date = BMF_date + 1 * BusinessDay()
    #     BMF_date = BMF_date + timedelta(days=1)

    # print(f'BMF after holiday {BMF_date.date()}')
    return BMF_date


##########################################
# DATA CONVERTER FOR TRADITION
#
def tradition_calc(picklefile):
    logger.info('TRADITION Data mining starting')
    logger.debug(f'Opening {picklefile}')
    df_tradition = joblib.load(picklefile)
    df_tradition = df_tradition[
        ['Trade_Date', 'Internal_Prod_ID', 'Internal_Prod_Des', 'Contract_Type', 'Total_Notional_USD_DA',
         'Total_Trade_Count', ]].copy()
    df_tradition = df_tradition.rename(columns={'Internal_Prod_ID': 'Description'})
    df_tradition = df_tradition.rename(columns={'Total_Notional_USD_DA': 'Volume'})

    # TYPES CONVERTION
    df_tradition = df_tradition.astype({'Volume': 'float'})

    # print('df describe\n', df_tradition.describe())

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

    # print('before', df_tradition)

    # PTAX, TOMPTAX, BMF
    for index, row in df_tradition.iterrows():
        CLASS_STATUS = None
        description = row['Description']
        # print(description)
        try:
            BRL = re.search(".*USDBRL_NDF.*", description)
        except:
            continue
        if BRL:
            # print(row['Description'])

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
            # print(f'DAYS:{DAYS} | expire_date:{expire_date} | today:{today.day} | bmf:{BMF_DATE} | delta:{delta_bmf} ')

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

    #####################
    # NUMBERS FOR HUMAN
    df_tradition_summary = df_tradition.groupby('Class').sum()

    number_to_human = []
    for number in df_tradition.groupby('Class').sum()['Volume']:
        converted = numerize.numerize(number)
        number_to_human.append(converted)

    df_tradition_summary['Vol'] = number_to_human

    #####################
    # DATAFRAME SORT
    df_tradition_summary = df_tradition_summary[['Vol', 'Volume']]

    # print('df_tradition',df_tradition)
    # print('df_tradition_summary',df_tradition_summary)

    # print(df_tradition_summary['Vol'])
    # print(df_tradition_summary.to_dict('dict'))
    # print(df_tradition_summary.to_dict('list'))

    return df_tradition_summary.to_dict('list')
