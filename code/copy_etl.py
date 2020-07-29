import os

import mysql.connector as mysql_con
from etlhelper import DbParams, execute, connect, copy_rows, get_rows
from settings import *

SHOST = S_HOST
SPORT = S_PORT
SDBNAME = S_DB_NAME
SDBUSER = S_DB_USER
SDBPASS = S_DB_PASSWORD
SDBENG = S_DB_ENGINE
S_TABLES = S_DB_TABLES

DHOST = D_HOST
DPORT = D_PORT
DDBNAME = D_DB_NAME
DDBUSER = D_DB_USER
DDBPASS = D_DB_PASSWORD
D_TABLE = D_DB_TABLE
DCLEAR = D_CLEAR

os.environ['S_PG_PASSWORD'] = SDBPASS
os.environ['D_PG_PASSWORD'] = DDBPASS

ENGINES = {
    'postgres': 'PG',
    'mysql': 'MSSQL',
    'oracle': 'ORACLE',
    'sqlite': 'SQLITE'
}

engine = ENGINES.get(SDBENG, 'PG')

if engine == ENGINES['mysql']:
    SOURCE_DB_PARAM = DbParams(dbtype=engine, host=SHOST, port=SPORT, dbname=SDBNAME, user=SDBUSER,
                               odbc_driver="ODBC Driver 17 for SQL Server")
elif engine == ENGINES['sqlite']:
    SOURCE_DB_PARAM = DbParams(dbtype=engine, filename='/path/to/file.db')
else:
    SOURCE_DB_PARAM = DbParams(dbtype=engine, host=SHOST, port=SPORT, dbname=SDBNAME, user=SDBUSER)

DESTINATION_DB_PARAMS = DbParams(dbtype='PG', host=DHOST, port=DPORT, dbname=DDBNAME, user=DDBUSER)


def copy_src_to_dest():
    delete_sql = table_delete_query(D_TABLE)  # USE THIS TO CLEAR DESTINATION FOR IDEMPOTENCE

    src_conn = get_source_connection()
    print('Connected to source')
    dest_conn = get_destination_connection()
    print('Connected to destination')

    s_tables = []
    if DCLEAR:
        execute(delete_sql, dest_conn)
        print('Cleared destination')

    if engine == ENGINES['mysql']:
        mysql_copy_src_to_dest(src_conn, s_tables, dest_conn)
    else:
        if S_TABLES == '__all__':
            tables_query = get_tables_query()
            rows = get_rows(tables_query, src_conn)
            for row in rows:
                s_tables.append(row.tablename)
        else:
            s_tables = S_TABLES.split(",")

        for S_DB_TABLE in s_tables:
            select_sql = table_select_query(S_DB_TABLE, src_conn)
            insert_sql = table_insert_query(D_TABLE)
            print('Copying data from %s' % S_DB_TABLE)
            copy_rows(select_sql, src_conn, insert_sql, dest_conn)
            create_view(S_DB_TABLE, dest_conn)

    refresh_mat_views(dest_conn)


def mysql_copy_src_to_dest(src_conn, s_tables, dest_conn):
    if S_TABLES == '__all__':
        tables_query = get_tables_query()
        rows = get_rows_func(tables_query, src_conn)
        for row in rows:
            s_tables.append(row[0])
    else:
        s_tables = S_TABLES.split(",")

    for S_DB_TABLE in s_tables:
        select_sql = table_select_query(S_DB_TABLE, src_conn)
        print('Copying data from %s' % S_DB_TABLE)
        cursor = src_conn.cursor()
        cursor.execute(select_sql)
        rows = cursor.fetchall()
        for row in rows:
            insert_sql = "INSERT INTO {0} (table_name, fields) VALUES ('{1}', '{2}')".format(D_TABLE, row[0], row[1])
            execute(insert_sql, dest_conn)

        create_view(S_DB_TABLE, dest_conn)


def get_source_connection():
    if engine == ENGINES['mysql']:
        return mysql_con.connect(host=SHOST, port=SPORT, user=SDBUSER, password=SDBPASS, database=SDBNAME)
    return connect(SOURCE_DB_PARAM, "S_PG_PASSWORD")


def get_destination_connection():
    return connect(DESTINATION_DB_PARAMS, "D_PG_PASSWORD")


def get_rows_func(query, conn):
    if engine == ENGINES['mysql']:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows
    rows = get_rows(query, conn)
    return rows


def table_delete_query(table):
    return "DELETE FROM {0} where true".format(table)


def get_tables_query():
    if engine == ENGINES['postgres']:
        return "select tablename from pg_catalog.pg_tables where schemaname = 'public';"
    elif engine == ENGINES['mysql']:
        return "SELECT TABLE_NAME as tablename FROM information_schema.tables where TABLE_SCHEMA = '{0}';".format(
            SDBNAME)
    return None


def table_select_query(table, conn):
    if engine == ENGINES['postgres']:
        return "select '{0}' as table_name , row_to_json(ROW(u))::text as json from {0} u".format(table)
    elif engine == ENGINES['mysql']:
        columns_query = "SELECT COLUMN_NAME as col_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{0}'".format(
            table)
        field_col_pair = ""
        rows = get_rows_func(columns_query, conn)

        for row in rows:
            field = row[0]
            field_col_pair += "'{0}', {0}, ".format(field)

        field_col_pair = field_col_pair[:-2]
        select_query = "SELECT '{0}' as table_name,  JSON_OBJECT('f1', JSON_OBJECT({1})) as json FROM {0};".format(
            table, field_col_pair)
        return select_query
    return None


def table_insert_query(table):
    return "INSERT INTO {0} (table_name, fields) VALUES (%s, %s)".format(table)


def view_count_query(view):
    return "select count(*) from pg_catalog.pg_views where schemaname = 'public' and viewname = '%s';" % view
    # return None


def sample_rows_query(view):
    return "select distinct on (1) table_name , fields as fields_json from {1} where table_name = '{0}' order by table_name ; ".format(
        view, D_TABLE)


def view_create_query(table, fields, view):
    return "create view {0} as select {1} from {2} where table_name = '{0}';".format(view, fields, table)


def create_view(view_name, dest_conn=None):
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
        rows = get_rows(sample_rows, dest_conn)
        if len(rows) > 0:
            print(rows[0])
            sample_json = rows[0].fields_json['f1']
            fields = ""
            for k, v in sample_json.items():
                fields += "fields #>> '{f1,%s}' as %s, " % (k, k)

            fields = fields[:-2]
            print('Creating view %s' % view_name)
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
        print('Refreshing materialized views')
        q = "select * from refresh_matviews();"
        execute(q, dest_conn)
        print('Done')
        return True
    return False


copy_src_to_dest()
