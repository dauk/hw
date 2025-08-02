from dim_category import load_dim_category
from dim_vendor import load_dim_vendor
from dim_host import load_dim_host
from fact_hosts import load_fact_hosts
from obt_hosts import load_obt_hosts
from stg_hosts import load_stg_hosts


def load_model():
    load_stg_hosts()#must load first
    load_dim_category()
    load_dim_vendor()
    load_dim_host()
    load_fact_hosts()
    load_obt_hosts()
    

if __name__ == "__main__":
    load_model()