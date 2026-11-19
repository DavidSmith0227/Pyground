import pandas as pd
import datetime as dt
from dateutil.parser import parse


# ---------------------------
# Data Type Check
# ---------------------------
def is_integer(value) -> bool:
    try:
        num = int(value)
        return True
    except ValueError:
        return False
    

def is_date(value) -> bool:
    try:
        parse(value)
        return True
    except ValueError:
        return False  


# ---------------------------
# Dates
# ---------------------------
def get_today(date_format = '%m/%d/%Y') -> str: 
    today = str(dt.date.today())
    formatted_date = reformat_date(today, date_format)
    return formatted_date


def reformat_date(date, return_format):
    """
    return_format: 
        examples = '%Y/%m/%d' = '2026/01/31', '%m/%d/%y' = '01/31/26' 
        %Y: Year with century (e.g., 2023)
        %y: 2 digit year (e.g. 26)
        %m: Month as a zero-padded decimal number (e.g., 01, 02, ..., 12)
        %d: Day of the month as a zero-padded decimal number (e.g., 01, 02, ..., 31)

        %a: Abbreviated weekday name (e.g., Sun, Mon)
        %A: Full weekday name (e.g., Sunday, Monday)

        %b: Abbreviated month name (e.g., Jan, Feb)
        %B: Full month name (e.g., January, February) 
    """

    date_parts = parse(date) # returns YYYY, MM, DD, H, S
    return_date = str(date_parts.strftime(return_format))
    return return_date
    

# ---------------------------
# strings
# ---------------------------
def return_str_none(obj_str):
    """
    returns None value if str == ''
    """
    if obj_str == '':
        return None
    else:
        return obj_str
    

