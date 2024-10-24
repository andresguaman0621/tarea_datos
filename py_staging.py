from extract.ext_countries import extraer_countries
from extract.ext_stores import extraer_stores
from extract.per_staging import persistir_staging
import traceback
import pandas as pd

try:
   
   print("Extrayendo datos de countries desde un CSV")
   countries = extraer_countries()
   print("Persistiendo en Staging datos de countries")

   persistir_staging(countries, 'ext_country')

   print("Extrayendo datos de stores desde una DB")
   stores = extraer_stores()
   print("Persistiendo en Staging datos de stores")

   persistir_staging(stores, 'ext_store')

except:
    traceback.print_exc()
finally:
    None