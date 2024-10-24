import traceback
from util.db_connection import Db_Connection
import pandas as pd
from sqlalchemy import BLOB

# Diccionario que mapea las tablas a las columnas que requieren tipos de datos específicos
binary_columns = {
    'address': {'location': BLOB},
    'staff': {'picture': BLOB},
    # Agrega otras tablas y columnas si es necesario
}

def persistir_staging(df_stg, tab_name):
    try:
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = '2003'
        db = 'staging'

        con_db = Db_Connection(type, host, port, user, pwd, db)
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos")
        
        # Obtener el nombre original de la tabla sin el prefijo 'ext_'
        original_tab_name = tab_name.replace('ext_', '')

        # Obtener los tipos de datos para la tabla si existen
        dtype = binary_columns.get(original_tab_name)

        # Persistir el DataFrame en la tabla correspondiente en staging
        df_stg.to_sql(tab_name, ses_db, if_exists='replace', index=False, dtype=dtype)

    except Exception as e:
        print(f"Error al persistir la tabla {tab_name}: {e}")
        traceback.print_exc()
    finally:
        pass

def extraer_y_persistir_en_staging():
    try:
        # Conexión a la base de datos OLTP
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = '2003'
        db = 'oltp'

        con_db = Db_Connection(type, host, port, user, pwd, db)
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos")

        # Lista de tablas que se deben extraer y luego persistir en staging
        tablas = [
            'actor', 'address', 'category', 'city', 'country',
            'customer', 'film', 'film_actor', 'film_category',
            'film_text', 'inventory', 'language', 'payment',
            'rental', 'staff', 'store'
        ]

        # Extraer y persistir cada tabla
        for tabla in tablas:
            try:
                # Extraer datos de la tabla en la base de datos OLTP
                query = f'SELECT * FROM {tabla}'
                df = pd.read_sql(query, ses_db)

                # Nombre de la tabla en staging con el prefijo 'ext_'
                staging_tabla = f'ext_{tabla}'

                # Persistir el DataFrame en la base de datos de staging
                persistir_staging(df, staging_tabla)
                print(f"Datos de la tabla {tabla} persistidos exitosamente en staging.")
            
            except Exception as e:
                print(f"Error al procesar la tabla {tabla}: {e}")
                traceback.print_exc()

    except Exception as e:
        print(f"Error general: {e}")
        traceback.print_exc()
    finally:
        pass

# Llamar a la función para extraer y persistir los datos en staging
if __name__ == "__main__":
    extraer_y_persistir_en_staging()
