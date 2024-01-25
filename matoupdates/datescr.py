from datetime import datetime, timedelta
import time

## name changes every half month
def get_name_datetime(date):
    if date.hour >=0 and date.hour < 4:
        ## move back date
        actudate = date - timedelta(hours=4,minutes=5)
    else:
        actudate = date
    
    datestr=f"{actudate.year}{actudate.month:02d}"
    if actudate.day >= 15:
        datestr+="15"
    else:
        datestr+="00"
    return  datestr

def make_basename_updates():
    return f"matoups_{get_name_datetime(datetime.now())}"

def day_hour_min_sec(date: datetime=None):
    if date is None:
        date = datetime.now()
    return f"{date.day:02d}{date.hour:02d}{date.minute:02d}{date.second:02d}"

ups_name_file = lambda basename: f"{basename}_{day_hour_min_sec()}.msgpk.zstd"

