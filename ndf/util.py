import re
from datetime import date, datetime, timedelta
import holidays
import os
from pandas._libs.tslibs.offsets import BDay, BMonthEnd


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

    # #print(f'BMF calculated {BMF_date.date()}')
    # BMF_date = BMF_date + 2 * BusinessDay()
    # BMF_date = BMF_date + timedelta(days=1)
    count = 1
    while (not is_weekday(BMF_date) or is_holiday(BMF_date)) or count < 2:
        BMF_date = BMF_date + timedelta(days=1)
        count += 1
    # #print(f'BMF after weekday {BMF_date.date()}')

    # while is_holiday(BMF_date):
    #     #print(f"{BMF_date.date()} is holiday")
    #     #BMF_date = BMF_date + 1 * BusinessDay()
    #     BMF_date = BMF_date + timedelta(days=1)

    # #print(f'BMF after holiday {BMF_date.date()}')
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
    bdate = None
    if year:
        bdate = datetime.today().replace(year=year, month=month, day=1)
    elif month and not year:
        bdate = datetime.today().replace(month=month, day=1)
    else:
        bdate = datetime.today().replace(day=1)

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

    return bdate


