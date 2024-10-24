from transform.tra_films import transformar_films   
import traceback
from util.db_connection import Db_Connection
import pandas as pd

def cargar_films():
    try:
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'root'
        pwd = '2003'
        db = 'staging'

        # Obtener los datos transformados desde la función transformar_films
        films_df = transformar_films()
        if films_df is None or films_df.empty:
            raise Exception("No hay datos de películas para cargar.")

        db = 'sor'

        # Conexión a la base de datos 'sor'
        con_sor_db = Db_Connection(type, host, port, user, pwd, db)
        ses_sor_db = con_sor_db.start()
        if ses_sor_db == -1:
            raise Exception("El tipo de base de datos dado no es válido.")
        elif ses_sor_db == -2:
            raise Exception("Error tratando de conectarse a la base de datos.")

        # Preparar el DataFrame para insertar en dim_film
        dim_film_df = films_df[['film_id', 'title', 'release_year', 'length', 'rating', 'duration']].copy()
        dim_film_df.rename(columns={'film_id': 'film_bk'}, inplace=True)

        # Asegurarse de que los tipos de datos coincidan con la tabla dim_film
        dim_film_df['film_bk'] = dim_film_df['film_bk'].astype('int16')
        dim_film_df['release_year'] = dim_film_df['release_year'].astype('int')
        dim_film_df['length'] = dim_film_df['length'].astype('int16')
        dim_film_df['rating'] = dim_film_df['rating'].astype(str)
        dim_film_df['duration'] = dim_film_df['duration'].astype(str)

        # Insertar los datos en la tabla dim_film
        dim_film_df.to_sql('dim_film', ses_sor_db, if_exists='append', index=False)
    except Exception as e:
        traceback.print_exc()
    finally:
        pass
