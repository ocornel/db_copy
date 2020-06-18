# Copy flattened table(s) from one server to another.

The code in this repo will enable you copy data from tables in one server database into another table/server database. This will flatten the original tables into two fields, table_name (string) and fields (jsonb) which you can later remodel using views. This transaction can happen between tables of same database, different databases, different servers etc provided you have the correct connection credentials.
  

## Setting up.  
The setup was prepared for linux ubuntu environments. You can however just figure out what the [setup.py](https://github.com/ocornel/db_copy/blob/master/code/setup.py) file does and implement manually on your OS
If you are on ubuntu, 
cd to the directory containing this `README.md` file then  

    $ python code/setup.py
What this does for you:

 1. Updates and Upgrades your apt packages
 2. Installs Python3.7,
 3. Installs Pip for python 3
 4. Installs build-essential unixodbc-dev
 5. Installs virtualenv, 
 6. sets up a new virtual env for this project and activates it
 7. Installs required pip packages into the virtual environment
So you can basically do these manually if you are on a different platform.

## Using
Once the setup is done, the usage is the same no matter the platform.

### Settings
The code needs to know which databases to target. so create a new file `local_settings.py` in the same directory as the `settings.py` and define the constants whose values you want to override from the [settings.py](https://github.com/ocornel/db_copy/blob/master/code/settings.py)
Example local_settings.py

    S_HOST = 'example.com'  
    S_DB_NAME = 'database'  
    S_DB_USER = 'user'  
    S_DB_PASSWORD = 'password'  
    S_DB_TABLES = "table1, table2, materialized_view1" # OR "__all__" if you want all tables  
      
    D_HOST = 'example2.com'  
    D_DB_NAME = 'database2'  
    D_DB_USER = 'user2'  
    D_DB_PASSWORD = 'password2'  
    D_DB_TABLE = 'destination_table'
    D_CLEAR = True
In this example local settings (compare with `settings.py`) we have maintained the ports for both db so we didn't repeat them but overiden everything else. We have instructed the app to clear the destination table by `D_CLEAR = True` on our local settings.
It's obvious that you ensure both source and destination parameters are valid lest you'll face errors.

### Running
    $python code/copy_etl.py
That will do the copy as in this example:
Source Table (students):

| id | first_name | last_name | email              | age |
|----|------------|-----------|--------------------|-----|
| 1  | Martin     | Cornel    | martin@example.com | 26  |
| 2  | Jane       | Doe       |                    | 22  |
| 3  | John       |           | john@d.net         |     |

Destination table (dumps):

| table_name | fields                                                                                           |
|------------|--------------------------------------------------------------------------------------------------|
| students   | {"f1":{"id":1,"first_name":"Martin","last_name":"Cornel","email":"martin@example.com","age":26}} |
| students   | {"f1":{"id":2,"first_name":"Jane","last_name":"Doe","age":22}}                                   |
| students   | {"f1":{"id":3,"first_name":"John","email":"john@d.net"}}                                         |

These can then be restructured using view queries to any form of table you can retrieve from the data using `fields#>>'{f1,first_name}' as first_name`

Update: The code now takes care of building the views in the destination database based on the source table.

##Build and run via docker
```docker build -t db_copy .```
```docker run -e S_DB_NAME='localhost' db_copy```


-- Martin Cornel.