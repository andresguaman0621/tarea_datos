# this file is a kind of python startup module used for manual unit testing

from util.db_connection import Db_Connection
import traceback
import pandas as pd

try:
    con_db = Db_Connection('mysql','localhost','3306','root','2003','oltp')
    ses_db = con_db.start()
    if ses_db == -1:
        raise Exception("El tipo de base de datos dado no es válido")
    elif ses_db == -2:
        raise Exception("Error tratando de conectarse a la base de datos ")
    
    databases = pd.read_sql ('show databases', ses_db)
    print (databases)
    
except:
    traceback.print_exc()
finally:
    pass
