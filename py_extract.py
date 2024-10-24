from extract.ext_countries import extraer_countries
from extract.ext_stores import extraer_stores
import traceback
import pandas as pd

try:
   
   # Extraccion CSV
   #print("Extrayendo datos de countries desde un CSV")
   #countries = extraer_countries()
   #print(countries)

   # Extraccion de la base de datos
   print("Extrayendo datos de stores desde una DB")
   stores = extraer_stores()
   print(stores)
except:
    traceback.print_exc()
finally:
    None