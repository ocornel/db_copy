import os
S_DB_NAME = os.environ['S_DB_NAME']
S_DB_USER = os.environ['S_DB_USER'] 
S_DB_PASSWORD = os.environ['S_DB_PASSWORD']
S_DB_TABLES = os.environ['S_DB_TABLES'].split(",")
  
D_HOST = os.environ['D_HOST']
D_DB_NAME = os.environ['D_DB_NAME']
D_DB_USER = os.environ['D_DB_USER'] 
D_DB_PASSWORD = os.environ['D_DB_PASSWORD'] 
D_DB_TABLE = os.environ['D_DB_TABLE']
D_CLEAR = os.environ['D_CLEAR']