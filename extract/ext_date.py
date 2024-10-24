import pandas as pd
import traceback

def extraer_dates():
    try:
        # Leer el archivo CSV
        filename = './csvs/dates.csv'
        dates = pd.read_csv(filename)

        dates = dates.rename(columns={
            'date_id': 'id',
            'date': 'date_bk',
            'month': 'date_month',
            'year': 'date_year'
        })

        return dates

    except Exception as e:
        traceback.print_exc()
    finally:
        pass
