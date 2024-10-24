
from extract.per_staging import persistir_staging
from extract.ext_inventories import extraer_inventories
from extract.ext_films import extraer_films

import traceback
import pandas as pd

try:
   print("Extrayendo datos de inventory desde OLTP")
   inventories = extraer_inventories()
   print("Persistiendo en Staging datos de inventory")
   persistir_staging(inventories, 'ext_inventory')

   print("Extrayendo datos de film desde OLTP")
   films = extraer_films()
   print("Persistiendo en Staging datos de film")
   persistir_staging(films, 'ext_film')

except:
    traceback.print_exc()
finally:
    None


