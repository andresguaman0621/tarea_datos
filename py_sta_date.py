
from extract.per_staging import persistir_staging
from extract.ext_date import extraer_dates

import traceback
import pandas as pd

try:
   print("Extrayendo datos de date desde un CSV")
   dates = extraer_dates()
   print("Persistiendo en Staging datos de date")
   persistir_staging(dates, 'ext_date')

except:
    traceback.print_exc()
finally:
    None


