import re
from datetime import date, datetime, timedelta
import holidays


def list_days_in_year(year):
    # Get first day of the year
    first_day = date(year, 1, 1)

    # Get last day of the year
    last_day = date(year, 12, 31)

    # Get all days in a year
    days = []
    while first_day <= last_day:
        days.append(first_day)
        first_day += timedelta(days=1)

    return days


# Get all weeks in a year
def get_weeks_in_year(year):
    # Get first day of the year
    first_day = date(year, 1, 1)

    # Get last day of the year
    last_day = date(year, 12, 31)

    # Get first monday of the year
    first_monday = first_day + timedelta(days=-first_day.weekday(), weeks=1)

    # Get last sunday of the year
    last_sunday = last_day + timedelta(days=-last_day.weekday(), weeks=1)

    # Get all weeks in a year
    weeks = []
    while first_monday <= last_sunday:
        weeks.append(first_monday)
        first_monday += timedelta(days=7)

    return weeks


# get_days_in_week
def get_days_in_week(week, year):
    # Get first day of the week
    first_day = week

    # Get last day of the week
    last_day = week + timedelta(days=6)

    # Get all days in a week
    days = []
    while first_day <= last_day:
        days.append(first_day)
        first_day += timedelta(days=1)

    return days


# Show all holidays in a year
def show_holidays_in_year(year):
    # Get all holidays from Brazil and US
    br_holidays = holidays.Brazil()
    us_holidays = holidays.UnitedStates()

    days = list_days_in_year(year)
    for day in days:
        # Check if day is a holiday
        if day in br_holidays:
            # print holyday description
            print(day, br_holidays.get(day))
            break

def get_week_number(date):
    return date.isocalendar()[1]

# Show me weeks when have Brazil and US holidays
def show_weeks_with_holidays(year):
    # Get all holidays from Brazil and US
    br_holidays = holidays.Brazil()
    us_holidays = holidays.UnitedStates()
    nyse_holidays = holidays.financial_holidays('NYSE')
    list_holidays = {}

    # Get all weeks in a year
    weeks = get_weeks_in_year(year)

    # Store in a list all days of the year
    days = list_days_in_year(year)

    for day in days:
        if day in br_holidays or day in us_holidays or day in nyse_holidays:
            week_number = get_week_number(day)
            print(day, week_number, br_holidays.get(day) or us_holidays.get(day))
            list_holidays[week_number] = day.strftime('%m/%d/%Y'), (br_holidays.get(day) or us_holidays.get(day))

    return list_holidays


if __name__ == "__main__":
    lista = show_weeks_with_holidays(2023)
    print(lista[52])
