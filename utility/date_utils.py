from datetime import datetime, timedelta

def get_date_glob(number_of_days:int)->str:
    """
    Generates a date glob pattern string for the past N days in "yyyy-MM-dd" format.

    This is useful for building S3 path expressions that match multiple dated folders or files.

    Args:
        number_of_days (int): The number of days to include, counting back from today.
                              For example, if 3 is passed and today is 2025-08-03,
                              it returns "{2025-08-03,2025-08-02,2025-08-01}".

    Returns:
        str: A comma-separated date glob string wrapped in curly braces, e.g., "{2025-08-03,2025-08-02,...}".
    """
    date_list = [(datetime.today() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(number_of_days)]
    date_glob = "{" + ",".join(date_list) + "}"
    return date_glob