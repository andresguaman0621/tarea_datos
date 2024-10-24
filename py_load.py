
from load.load_dates import cargar_dates
from load.load_films import cargar_films
from load.load_inventory import cargar_fact_inventory
import traceback
import pandas as pd

try:
   
   print("Cargando datos de dates en SOR")
   cargar_dates()

   print("Cargando datos de films en SOR")
   cargar_films()

   print("Cargando datos para la tabla fact_inventory en SOR")
   cargar_fact_inventory()
except:
    traceback.print_exc()
finally:
    None


