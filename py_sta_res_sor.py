from transform.tra_films import transformar_films
from extract.per_staging import persistir_staging
import traceback

try:
    print("Transformando datos de films EN EL STAGING")
    tra_films = transformar_films()
    print(tra_films)

except Exception as e:
    traceback.print_exc()
finally:
    None
