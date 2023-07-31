from datetime import datetime, timedelta

## name changes every half month
def get_name_datetime(date):
    if date.hour >0 and date.hour < 4:
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