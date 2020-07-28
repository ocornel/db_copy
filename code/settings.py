# SOURCE PARAMS
S_HOST = ''
S_PORT = 5432
S_DB_NAME = ''
S_DB_USER = ''
S_DB_PASSWORD = ''
S_DB_TABLES = ''
S_DB_ENGINE = 'postgres'

# DESTINATION PARAMS
D_HOST = ''
D_PORT = 5432
D_DB_NAME = ''
D_DB_USER = ''
D_DB_PASSWORD = ''
D_DB_TABLE = ''
D_CLEAR = False


# to override these settings, create local_settings and put in key value pairs that override these.
try:
    from local_settings import *
except ImportError:
    pass

