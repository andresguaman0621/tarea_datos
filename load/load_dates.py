import traceback
from util.db_connection import Db_Connection
import pandas as pd

def cargar_dates():
    try:
        tipo = 'mysql'
        host = 'localhost'
        puerto = '3306'
        usuario = 'root'
        contraseña = '2003'
        bd_staging = 'staging'
        bd_sor = 'sor'

        # Conexión a la base de datos staging
        con_sta_db = Db_Connection(tipo, host, puerto, usuario, contraseña, bd_staging)
        ses_sta_db = con_sta_db.start()
        if ses_sta_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sta_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos staging")

        sql_stmt = "SELECT id, date_bk, date_month, date_year FROM ext_date"
        dates_ext = pd.read_sql(sql_stmt, ses_sta_db)

        # Conexión a la base de datos sor
        con_sor_db = Db_Connection(tipo, host, puerto, usuario, contraseña, bd_sor)
        ses_sor_db = con_sor_db.start()
        if ses_sor_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_sor_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos sor")

        if not dates_ext.empty:
            dates_ext.to_sql('dim_date', ses_sor_db, if_exists='append', index=False)
    except:
        traceback.print_exc()
    finally:
        pass
