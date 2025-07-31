from datetime import datetime, timedelta

def get_date_glob(number_of_days:int)->str:
    date_list = [(datetime.today() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(number_of_days)]
    date_glob = "{" + ",".join(date_list) + "}"
    return date_glob

print(get_date_glob(3))