from transform.tra_stores import transformar_stores
from extract.ext_countries import extraer_countries
from extract.ext_stores import extraer_stores
from extract.per_staging import persistir_staging
from extract.ext_addresses import extraer_addresses
from extract.ext_cities import extraer_cities
from load.load_stores import cargar_stores
import traceback
import pandas as pd

try:
   print("Extrayendo datos de countries desde un CSV")
   countries = extraer_countries()
   print("Persistiendo en Staging datos de countries")
   persistir_staging(countries, 'ext_country')

   print("Extrayendo datos de stores desde OLTP")
   stores = extraer_stores()
   print("Persistiendo en Staging datos de stores")
   persistir_staging(stores, 'ext_store')

   print("Extrayendo datos de addresses desde OLTP")
   addresses = extraer_addresses()
   print("Persistiendo en Staging datos de addresses")
   persistir_staging(addresses, 'ext_address')

   print("Extrayendo datos de cities desde OLTP")
   cities = extraer_cities()
   print("Persistiendo en Staging datos de cities")
   persistir_staging(cities, 'ext_city')

   
   print("Transformando datos de stores EN EL STAGING")
   tra_stores = transformar_stores()
   #print(tra_stores)
   print("Persistiendo en Staging datos transformados de stores")
   persistir_staging(tra_stores, 'tra_store')

   print("Cargando datos de stores en SOR")
   cargar_stores()

except:
    traceback.print_exc()
finally:
    None


