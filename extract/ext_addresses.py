import traceback
from util.db_connection import Db_Connection
import pandas as pd

def extraer_addresses():
    try:
        # Parámetros de conexión
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = '2003'
        db = 'oltp'

        # Establecer conexión
        con_db = Db_Connection(type, host, port, user, pwd, db)
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos")
        
        # Consulta SQL con conversión de 'location' a texto
        query = '''
            SELECT 
                address_id, 
                address, 
                address2, 
                district, 
                city_id, 
                postal_code, 
                phone, 
                ST_AsText(location) AS location, 
                last_update 
            FROM address
        '''
        addresses = pd.read_sql(query, ses_db)

        return addresses

    except Exception as e:
        print(f"Error al extraer addresses: {e}")
        traceback.print_exc()
    finally:
        pass

# Llamar a la función y mostrar los primeros registros
if __name__ == "__main__":
    df_addresses = extraer_addresses()
    print(df_addresses.head())
