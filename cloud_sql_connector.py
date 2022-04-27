import datetime
import logging
import os
import sqlalchemy
from sqlalchemy import Column, Table, MetaData, String, Integer, Numeric

logger = logging.getLogger()

def init_connection_engine():
    db_config = {
        # Pool size is the maximum number of permanent connections to keep.
        "pool_size": 5,
        # Temporarily exceeds the set pool_size if no connections are available.
        "max_overflow": 2,
        # The total number of concurrent connections for your application will be
        # a total of pool_size and max_overflow.

        "pool_timeout": 30,  # 30 seconds

        # 'pool_recycle' is the maximum number of seconds a connection can persist.
        # Connections that live longer than the specified amount of time will be
        # reestablished
        "pool_recycle": 300,  # 5 minutes
    }

    return init_unix_connection_engine(db_config)


def init_unix_connection_engine(db_config):
    # [START cloud_sql_mysql_sqlalchemy_create_socket]
    # Remember - storing secrets in plaintext is potentially unsafe. Consider using
    # something like https://cloud.google.com/secret-manager/docs/overview to help keep
    # secrets secret.
    db_user = os.environ["DB_USER"]
    db_pass = os.environ["DB_PASS"]
    db_name = os.environ["DB_NAME"]
    db_socket_dir = os.environ.get("DB_SOCKET_DIR", "./cloudsql")
    cloud_sql_connection_name = os.environ["CLOUD_SQL_CONNECTION_NAME"]

    pool = sqlalchemy.create_engine(
        # Equivalent URL:
        # mysql+pymysql://<db_user>:<db_pass>@/<db_name>?unix_socket=<socket_path>/<cloud_sql_instance_name>
        sqlalchemy.engine.url.URL(
            drivername="mysql+pymysql",
            username=db_user,  # e.g. "my-database-user"
            password=db_pass,  # e.g. "my-database-password"
            database=db_name,  # e.g. "my-database-name"
            query={
                "unix_socket": "{}/{}".format(
                    db_socket_dir,  # e.g. "/cloudsql"
                    cloud_sql_connection_name)  # i.e "<PROJECT-NAME>:<INSTANCE-REGION>:<INSTANCE-NAME>"
            }
        ),
        **db_config
    )
    # [END cloud_sql_mysql_sqlalchemy_create_socket]

    return pool

def get_bmw_table():
    meta = MetaData()
    bmw_table = Table('bmw', meta, \
             Column('id_', String, primary_key = True), Column('no', String), Column('source_no', String), \
             Column('fac_year', Integer), Column('fac_month', Integer), \
             Column('lic_year', Integer), Column('lic_month', Integer), \
             Column('mileage', Numeric), Column('displacement', Integer), Column('price', Numeric), \
             Column('model_name', String), Column('model_type', String), \
             Column('series_id', String), Column('series_name', String), \
             Column('show_name', String), Column('store', String), Column('phone', String), \
             Column('model_engine', String), \
             Column('kit_name', String), Column('color_name', String), \
             Column('status_name', String), Column('memo', String))
    return bmw_table

def process_crawl_result(db, bmw_table, resp_dict):
    newly_added_cars = {}
    for car in resp_dict['car']:
        id_ = str(car['id'])
        no = car['no']
        source_no = car['source_no']
        fac_year = car['fac_year']
        fac_month = car['fac_month']
        lic_year = car['lic_year']
        lic_month = car['lic_month']
        mileage = car['mileage']
        displacement = car['displacement']
        price = car['price']
        model_name = car['model_name']
        model_type = car['model_type']
    
        series_id = car['series_id']
        series_name = car['series_name']
        show_name = car['show_name']
        store = car['store']
        phone = car['phone']
        model_engine = car['model_engine']
        kit_name = car['kit_name']
        color_name = car['color_name']
        status_name = car['status_name']
        memo = car['memo']

        # Check if the car is already in the inventory
        # If not, then we write it in the DB
        result = None
        query = bmw_table.select().where(bmw_table.c.id_==id_)
        with db.connect() as conn:
            result = conn.execute(query)
        
        if len(result.fetchall()) == 0:
            newly_added_cars[id_] = car
            ins = bmw_table.insert().values(id_=id_, no=no, source_no=source_no, \
                          fac_year=fac_year, fac_month=fac_month, lic_year=lic_year, lic_month=lic_month, \
                          mileage=mileage, displacement=displacement, price=price, \
                          model_name=model_name, model_type=model_type, \
                          series_id=series_id, series_name=series_name, \
                          show_name=show_name, store=store, phone=phone, model_engine=model_engine, \
                          kit_name=kit_name, color_name=color_name, status_name=status_name, memo=memo)

            with db.connect() as conn:
                conn.execute(ins)

    return newly_added_cars