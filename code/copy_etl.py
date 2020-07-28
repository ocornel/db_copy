import json
import os

from etlhelper import DbParams, execute, connect, copy_rows, get_rows
from settings import *

os.environ['S_PG_PASSWORD'] = S_DB_PASSWORD
os.environ['D_PG_PASSWORD'] = D_DB_PASSWORD

ENGINES = {
    'postgres': 'PG',
    'mysql': 'MSSQL',
    'oracle': 'ORACLE',
    'sqlite': 'SQLITE'
}

engine = ENGINES.get(S_DB_ENGINE, 'PG')

SOURCE_PG_DB = DbParams(dbtype=engine, host=S_HOST, port=S_PORT, dbname=S_DB_NAME, user=S_DB_USER)
DESTINATION_PG_DB = DbParams(dbtype=engine, host=D_HOST, port=D_PORT, dbname=D_DB_NAME, user=D_DB_USER)
S_DB_TABLES = S_DB_TABLES
D_TABLE = D_DB_TABLE


def copy_src_to_dest():
    delete_sql = table_delete_query(D_TABLE)  # USE THIS TO CLEAR DESTINATION FOR IDEMPOTENCE

    with connect(SOURCE_PG_DB, "S_PG_PASSWORD") as src_conn:
        with connect(DESTINATION_PG_DB, "D_PG_PASSWORD") as dest_conn:
            s_tables = []
            if D_CLEAR:
                execute(delete_sql, dest_conn)

            if S_DB_TABLES == '__all__':
                tables_query = get_tables_query()
                rows = get_rows(tables_query, src_conn)
                for row in rows:
                    s_tables.append(row.tablename)

            else:
                s_tables = S_DB_TABLES.split(",")

            for S_DB_TABLE in s_tables:
                select_sql = table_select_query(S_DB_TABLE)
                insert_sql = table_insert_query(D_TABLE)
                copy_rows(select_sql, src_conn, insert_sql, dest_conn)

                create_view(S_DB_TABLE, src_conn, dest_conn)

            if engine == ENGINES['postgres']:
                refresh_mat_views(dest_conn)


def table_delete_query(table):
    if engine == ENGINES['postgres']:
        return "DELETE FROM {0}".format(table)
    return None


def get_tables_query():
    if engine == ENGINES['postgres']:
        return "select tablename from pg_catalog.pg_tables where schemaname = 'public';"
    return None


def table_select_query(table):
    if engine == ENGINES['postgres']:
        return "select '{0}' as table_name , row_to_json(ROW(u))::text as json from {0} u".format(table)
    return None


def table_insert_query(table):
    if engine == ENGINES['postgres']:
        return "INSERT INTO {0} (table_name, fields) VALUES (%s, %s)".format(table)
    return None


def view_count_query(view):
    if engine == ENGINES['postgres']:
        return "select count(*) from pg_catalog.pg_views where schemaname = 'public' and viewname = '%s';" % view
    return None


def sample_rows_query(view):
    if engine == ENGINES['postgres']:
        return "select distinct on (1) '{0}' as table_name , row_to_json(ROW(u))::text as json from {0} u order by table_name ; ".format(
            view)
    return None


def view_create_query(table, fields, view):
    if engine == ENGINES['postgres']:
        return "create view {0} as select {1} from {2} where table_name = '{0}';".format(view, fields, table)
    return None


def create_view(view_name, src_conn=None, dest_conn=None):
    """
    Checks if view exists before getting a sample json and creating a view based on the table_name
        :param view_name: table to sample from...also name of destination view to be created
        :param dest_conn: Destination connection
        :param src_conn: Source connection
        :return: True if view created, false if not
    """
    view_count_sql = view_count_query(view_name)
    view_count = get_rows(view_count_sql, dest_conn)[0].count
    if view_count == 0:
        sample_rows = sample_rows_query(view_name)
        rows = get_rows(sample_rows, src_conn)
        if len(rows) > 0:
            sample_json = json.loads(rows[0].json)['f1']
            fields = ""
            for k, v in sample_json.items():
                fields += "fields #>> '{f1,%s}' as %s, " % (k, k)

            fields = fields[:-2]
            view_sql = view_create_query(D_TABLE, fields, view_name)
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
