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

    # with connect(SOURCE_DB_PARAM, "S_PG_PASSWORD") as src_conn:
    #     with connect(DESTINATION_DB_PARAMS, "D_PG_PASSWORD") as dest_conn:
    s_tables = []
    if DCLEAR:
        execute(delete_sql, dest_conn)
        print('Cleared destination')

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
        create_view(S_DB_TABLE, src_conn, dest_conn)

    # if engine == ENGINES['postgres']:
    refresh_mat_views(dest_conn)


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
        res = []
        for row in rows:
            res.append(Row(tablename=row[0]))


        return rows
    rows = get_rows(query, conn)
    print(rows)
    return rows


def table_delete_query(table):
    # if engine == ENGINES['postgres']:
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

        # field_col_pair = "'id', id, 'firstName', first_name, 'lastName', last_name"
        field_col_pair = ""
        rows = get_rows(columns_query, conn)

        for row in rows:
            field = row.col_name
            field_col_pair += "'{0}', {0}, ".format(field)

        field_col_pair = field_col_pair[:-2]
        select_query = "SELECT '{0}' as table_name,  JSON_OBJECT('f1', JSON_OBJECT({1})) as json FROM {0};".format(
            table, field_col_pair)
        return select_query
    return None


def table_insert_query(table):
    # if engine == ENGINES['postgres']: # destination always postgres
    return "INSERT INTO {0} (table_name, fields) VALUES (%s, %s)".format(table)


def view_count_query(view):
    # if engine == ENGINES['postgres']:
    return "select count(*) from pg_catalog.pg_views where schemaname = 'public' and viewname = '%s';" % view
    # return None


def sample_rows_query(view):
    # if engine == ENGINES['postgres']:
    return "select distinct on (1) table_name , fields as fields_json from {1} where table_name = '{0}' order by table_name ; ".format(
        view, D_TABLE)


def view_create_query(table, fields, view):
    # if engine == ENGINES['postgres']:
    return "create view {0} as select {1} from {2} where table_name = '{0}';".format(view, fields, table)
    # return None


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
        rows = get_rows(sample_rows, dest_conn)
        if len(rows) > 0:
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



[('account_periods',), ('accounting_period_balances',), ('actual_agent_businesses',), ('africas_talking_callbacks',), ('agent_classes',), ('agents',), ('alerts',), ('approval_roles',), ('approvals',), ('audits',), ('bank_agreements',), ('bank_branches',), ('bank_deposits',), ('banking_instruments',), ('banking_schedules',), ('banks',), ('black_listed_clients',), ('borrowing_instruments',), ('borrowing_schedules',), ('bounced_payments',), ('budget_targets',), ('budgets',), ('business_rules',), ('cheque_statuses',), ('cheques',), ('client_obligation_statuses',), ('client_obligations',), ('clients',), ('communication_recipients',), ('communication_set_ups',), ('contact_people',), ('crb_scores',), ('crbs',), ('credit_note_usages',), ('credit_notes',), ('dashboard_item_roles',), ('dashboard_items',), ('dashboard_trends',), ('data_migrations',), ('debit_notes',), ('direct_debits',), ('disbursement_schedule_ipf_loans',), ('disbursement_schedules',), ('discount_matrices',), ('env_files',), ('ext_bank_accounts',), ('gl_accounts',), ('gl_entries',), ('incentive_setups',), ('incentives',), ('incoming_cheques',), ('insurer_agreements',), ('insurers',), ('integration_wallet_balances',), ('ipf_discounts',), ('ipf_first_payments',), ('ipf_loan_applications',), ('ipf_loan_approval_limits',), ('ipf_loan_gradings',), ('ipf_loan_statuses',), ('ipf_product_types',), ('ipf_products',), ('jobs',), ('lender_agreements',), ('lenders',), ('loan_agreements',), ('loan_gradings',), ('loans',), ('maker_checker_comments',), ('maker_checker_roles',), ('maker_checkers',), ('manual_journals',), ('manual_matchings',), ('metropol_crbs',), ('migrations',), ('mpesa_call_backs',), ('mpesa_calls',), ('mpesa_payments',), ('out_going_cheques',), ('parameters',), ('password_resets',), ('password_securities',), ('pay_bill_balances',), ('payment_voucher_payments',), ('payment_vouchers',), ('priviledges',), ('product_interest_rates',), ('proratas',), ('public_holiday_years',), ('public_holidays',), ('quotations',), ('rcl_bank_accounts',), ('rcl_obligations',), ('rcl_outputs',), ('recommendations',), ('refunds',), ('replacements',), ('report_roles',), ('report_setups',), ('reports',), ('role_priviledges',), ('roles',), ('sla_actual_averages',), ('slas',), ('sms',), ('staged_approval_comments',), ('staged_approvals',), ('staged_settings',), ('subsidiary_accounts',), ('system_emails',), ('trans_unions',), ('transactions',), ('tutorials',), ('user_incentive_per_ipfs',), ('users',)]


[Row(tablename='django_migrations'), Row(tablename='django_content_type'), Row(tablename='qed_suppliersectionscore'), Row(tablename='auth_group_permissions'), Row(tablename='auth_group'), Row(tablename='auth_user_groups'), Row(tablename='auth_permission'), Row(tablename='auth_user_user_permissions'), Row(tablename='django_admin_log'), Row(tablename='django_pesapal_transaction'), Row(tablename='qed_buyerprivilege'), Row(tablename='qed_buyerroleprivilege'), Row(tablename='qed_categorygroup'), Row(tablename='qed_contractdocument'), Row(tablename='qed_contractsectioncategorytype'), Row(tablename='qed_qedprivilege'), Row(tablename='qed_qedroleprivilege'), Row(tablename='qed_qualityassurance'), Row(tablename='oauth2_provider_accesstoken'), Row(tablename='qed_rfqitem'), Row(tablename='oauth2_provider_application'), Row(tablename='qed_setting'), Row(tablename='oauth2_provider_grant'), Row(tablename='oauth2_provider_refreshtoken'), Row(tablename='qed_country'), Row(tablename='qed_currency'), Row(tablename='qed_mpesacallbacks'), Row(tablename='qed_mpesacalls'), Row(tablename='qed_mpesapayment'), Row(tablename='qed_paybillballance'), Row(tablename='qed_pesapalcall'), Row(tablename='qed_pesapalcallback'), Row(tablename='qed_pesapalpayment'), Row(tablename='qed_supplierprequalscore'), Row(tablename='qed_supplierrfqtotal'), Row(tablename='qed_suppliertasksubmission'), Row(tablename='qed_prequalification'), Row(tablename='qed_requestforquotation'), Row(tablename='qed_useraudit'), Row(tablename='qed_tender'), Row(tablename='qed_suppliertenderscore'), Row(tablename='qed_rfqitemresponse'), Row(tablename='qed_requestforquotationinvitee'), Row(tablename='qed_regretletter'), Row(tablename='qed_questionreuse'), Row(tablename='qed_section'), Row(tablename='qed_qualityassuranceresponse'), Row(tablename='qed_qualityassurancequestion'), Row(tablename='qed_qedrole'), Row(tablename='qed_qed'), Row(tablename='qed_prequalificationscore'), Row(tablename='qed_supplierresponse'), Row(tablename='qed_otherresponse'), Row(tablename='qed_question'), Row(tablename='qed_markingscheme'), Row(tablename='qed_sourcingactivity'), Row(tablename='qed_duediligencesupplier'), Row(tablename='qed_duediligencequestion'), Row(tablename='auth_user'), Row(tablename='qed_duediligence'), Row(tablename='qed_contractsection'), Row(tablename='qed_contractcontractsection'), Row(tablename='qed_contract'), Row(tablename='qed_companysupplier'), Row(tablename='qed_categorysupplierpayment'), Row(tablename='qed_payment'), Row(tablename='qed_supplierprofile'), Row(tablename='qed_categoryorder'), Row(tablename='qed_categorytype'), Row(tablename='qed_job'), Row(tablename='qed_buyerrole'), Row(tablename='qed_company'), Row(tablename='qed_buyer'), Row(tablename='qed_category'), Row(tablename='qed_supplier'), Row(tablename='qed_awardletter'), Row(tablename='django_session'), Row(tablename='django_site')]
