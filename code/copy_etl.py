import json
import os

from etlhelper import DbParams, execute, connect, copy_rows, get_rows
from settings import *

os.environ['S_PG_PASSWORD'] = S_DB_PASSWORD
os.environ['D_PG_PASSWORD'] = D_DB_PASSWORD

SOURCE_PG_DB = DbParams(dbtype='PG', host=S_HOST, port=S_PORT, dbname=S_DB_NAME, user=S_DB_USER)
DESTINATION_PG_DB = DbParams(dbtype='PG', host=D_HOST, port=D_PORT, dbname=D_DB_NAME, user=D_DB_USER)
D_TABLE = D_DB_TABLE


def copy_src_to_dest():
    delete_sql = "DELETE FROM {0}".format(D_TABLE)  # USE THIS TO CLEAR DESTINATION FOR IDEMPOTENCE

    with connect(SOURCE_PG_DB, "S_PG_PASSWORD") as src_conn:
        with connect(DESTINATION_PG_DB, "D_PG_PASSWORD") as dest_conn:
            if D_CLEAR:
                execute(delete_sql, dest_conn)
            for S_DB_TABLE in S_DB_TABLES:
                select_sql = "select '{0}' as table_name , row_to_json(ROW(u))::text as json from {0} u".format(
                    S_DB_TABLE)
                insert_sql = "INSERT INTO {0} (table_name, fields) VALUES (%s, %s)".format(D_TABLE)
                copy_rows(select_sql, src_conn, insert_sql, dest_conn)

                create_view(S_DB_TABLE, src_conn, dest_conn)
            refresh_mat_views(dest_conn)


def create_view(view_name, src_conn=None, dest_conn=None):
    """
    Checks if view exists before getting a sample json and creating a view based on the table_name
        :param view_name: table to sample from...also name of destination view to be created
        :param dest_conn: Destination connection
        :param src_conn: Source connection
        :return: True if view created, false if not
    """
    count_query = "select count(*) from pg_catalog.pg_views where schemaname = 'public' and viewname = '%s';" % view_name
    view_count = get_rows(count_query, dest_conn)[0].count
    if view_count == 0:
        select_sql = "select distinct on (1) '{0}' as table_name , row_to_json(ROW(u))::text as json from {0} u order by table_name ; ".format(
            view_name)

        sample_json = json.loads(get_rows(select_sql, src_conn)[0].json)['f1']
        fields = ""
        for k, v in sample_json.items():
            fields += "fields #>> '{f1,%s}' as %s, " % (k, k)

        fields = fields[:-2]
        view_sql = "create view {0} as select {1} from {2} where table_name = '{0}';".format(view_name, fields, D_TABLE)
        execute(view_sql, dest_conn)
        return True
    return False


def refresh_mat_views(dest_conn):
    """
    Checks if refresh_matviews() function is defined in destination database before calling it
    :param dest_conn: Destination connection
    :return: True if materialized views refreshed, false if not.
    """
    count_query = "SELECT count(*)FROM information_schema.routines where routine_name = 'refresh_matviews';"
    counts = get_rows(count_query, dest_conn)[0].count
    if counts > 0:
        q = "select * from refresh_matviews();"
        execute(q, dest_conn)
        return True
    return False


copy_src_to_dest()
