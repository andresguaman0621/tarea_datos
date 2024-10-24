import traceback
from util.db_connection import Db_Connection
import pandas as pd

def transformar_films():
    try:
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = '2003'
        db = 'staging'

        # Conexión a la base de datos
        con_db = Db_Connection(type, host, port, user, pwd, db)
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception("El tipo de base de datos dado no es válido")
        elif ses_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos")

        # Consulta SQL para extraer solo las columnas necesarias de ext_films
        sql_stmt = "SELECT film_id, title, release_year, length, rating FROM ext_film"

        # Leer datos en un DataFrame de pandas
        films_df = pd.read_sql(sql_stmt, ses_db)

        # Función para categorizar la duración
        def categorizar_longitud(length):
            if length < 60:
                return '< 1h'
            elif length < 90:
                return '< 1.5h'
            elif length < 120:
                return '< 2h'
            else:
                return '> 2h'

        # Aplicar la función de categorización y crear la columna 'duration'
        films_df['duration'] = films_df['length'].apply(categorizar_longitud)

        return films_df

    except Exception as e:
        traceback.print_exc()
    finally:
        pass
