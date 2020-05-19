import os

from etlhelper import DbParams, execute, connect, copy_rows
from settings import *

os.environ['S_PG_PASSWORD'] = S_DB_PASSWORD
os.environ['D_PG_PASSWORD'] = D_DB_PASSWORD

SOURCE_PG_DB = DbParams(dbtype='PG', host=S_HOST, port=S_PORT, dbname=S_DB_NAME, user=S_DB_USER)
DESTINATION_PG_DB = DbParams(dbtype='PG', host=D_HOST, port=D_PORT, dbname=D_DB_NAME, user=D_DB_USER)


def copy_src_to_dest():
    delete_sql = "DELETE FROM {0}".format(D_DB_TABLE)  # USE THIS TO CLEAR DESTINATION FOR IDEMPOTENCE
    select_sql = "select '{0}' as table_name , row_to_json(ROW(u))::text as json from {0} u".format(S_DB_TABLE)
    insert_sql = "INSERT INTO {0} (table_name, fields) VALUES (%s, %s)".format(D_DB_TABLE)

    with connect(SOURCE_PG_DB, "S_PG_PASSWORD") as src_conn:
        with connect(DESTINATION_PG_DB, "D_PG_PASSWORD") as dest_conn:
            if D_CLEAR:
                execute(delete_sql, dest_conn)
            copy_rows(select_sql, src_conn,
                      insert_sql, dest_conn)


copy_src_to_dest()
