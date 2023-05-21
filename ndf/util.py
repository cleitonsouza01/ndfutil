import re
from datetime import date, datetime, timedelta
import holidays
import os
from pandas._libs.tslibs.offsets import BDay, BMonthEnd
from dateutil.relativedelta import relativedelta


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


def is_yesterday_weekday():
    yesterday = datetime.today() - timedelta(days=1)
    weekno = yesterday.weekday()
    result = None
    if weekno < 5:
        result = True
    else:
        # 5 Sat, 6 Sun
        result = False
    return result


# Setup file name
def get_cache_filename(source, date=None):
    today_date = datetime.now().strftime('%Y-%m-%d') if date is None else date
    file = f"{source}_{today_date}.pkl"
    file_with_path = os.path.join(os.getcwd() + os.sep + 'DATA' + os.sep, file)
    return file_with_path


def is_weekday(date):
    weekno = date.weekday()
    result = None
    if weekno < 5:
        result = True
    else:
        # 5 Sat, 6 Sun
        result = False
    return result


def is_weekend(date):
    weekno = date.weekday()
    return True if weekno >= 5 else False


def get_last_business_day(test_date=None):
    test_date = test_date - timedelta(days=1) if test_date else datetime.now()
    while is_weekend(test_date) or is_holiday(test_date):
        test_date = test_date - timedelta(days=1)

    return test_date


#####################
# GET BMF DATE
# BMF
# É o segundo dia util do mes seguinte. Feriados Brasileiros OU americanos não é dia util.
# Quando dizemos BMF sempre quer dizer do mes seguinte, BMF2, BMF3, BMF4... é referente aos meses subsequentes
def get_BMF_date(date=None):
    if not date:
        vdate = datetime.today()
        if vdate.day > get_second_business_day(vdate.month, vdate.year).day:
            vdate = vdate + relativedelta(months=+1)
    else:
        vdate = date
        BMF_date = get_second_business_day(vdate.month, vdate.year)
        if vdate.day > BMF_date.day:
            vdate = vdate + relativedelta(months=+1)
            BMF_date = get_second_business_day(vdate.month, vdate.year)

    BMF_date = get_second_business_day(vdate.month, vdate.year)
    return BMF_date


def human_format(num):
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    # add more suffixes if you need them
    return '%.2f%s' % (num, ['', 'K', 'M', 'G', 'T', 'P'][magnitude])


get_last_bd = lambda date: date.replace(day=1) + BMonthEnd()


def get_second_business_day(month=None, year=None):
    month = int(month) if month else None
    year = int(year) if year else None
    today = datetime.today()
    bdate = None
    if year:
        # bdate = datetime.strptime(f'{year}{month}01', '%Y%m%d')
        bdate = today.replace(year=year, month=month, day=1)
    elif month and not year:
        # bdate = datetime.strptime(f'{today.year}{month}01', '%Y%m%d')
        bdate = today.replace(month=month, day=1)
    else:
        # bdate = datetime.strptime(f'{today.year}{today.month}01', '%Y%m%d')
        bdate = today + timedelta(days=+28)
        bdate = bdate.replace(day=1)

    # print(bdate)
    if is_weekday(bdate):
        if not is_holiday(bdate):
            bdate = bdate + BDay(1)
        else:
            bdate = bdate + BDay(2)
    else:
        bdate = bdate + BDay(2)

    while is_holiday(bdate):
        bdate = bdate + BDay(1)

    bdate = datetime.fromtimestamp(datetime.timestamp(bdate))
    bdate = datetime(bdate.year, bdate.month, bdate.day)

    return bdate


