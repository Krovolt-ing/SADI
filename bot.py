import requests
import pandas as pd
import os
from datetime import datetime

FILE_NAME = "sadi_historico.csv"
URL_API = "https://api.cammesa.com/demanda-svc/demanda/ObtieneDemandaYTemperaturaRegion?id_region=1002"

def actualizar_csv():
    print(f"Iniciando captura: {datetime.now()}")
    try:
        res = requests.get(URL_API, timeout=20)
        print(f"Status API: {res.status_code}")
        
        data = res.json()
        if not data:
            print("API retornó lista vacía.")
            return

        df_raw = pd.DataFrame(data)
        df_real = df_raw.dropna(subset=['demHoy'])
        
        if df_real.empty:
            print("No hay datos de demanda 'demHoy' disponibles en este momento.")
            return

        ultimo = df_real.iloc[-1]
        fecha_str = pd.to_datetime(ultimo['fecha']).strftime("%Y-%m-%d %H:%M:%S")
        
        nuevo_dato = {
            "fecha": fecha_str,
            "demanda": int(ultimo['demHoy']),
            "prevista": int(ultimo.get('demPrevista', 0) or 0),
            "temperatura": float(ultimo['tempHoy'])
        }

        if not os.path.isfile(FILE_NAME):
            pd.DataFrame([nuevo_dato]).to_csv(FILE_NAME, index=False)
            print("Archivo creado con éxito.")
        else:
            df = pd.read_csv(FILE_NAME)
            if nuevo_dato['fecha'] not in df['fecha'].values:
                pd.concat([df, pd.DataFrame([nuevo_dato])], ignore_index=True).to_csv(FILE_NAME, index=False)
                print(f"Dato nuevo guardado: {fecha_str}")
            else:
                print("Dato duplicado, no se guarda.")
                
    except Exception as e:
        print(f"ERROR CRÍTICO: {e}")

if __name__ == "__main__":
    actualizar_csv()
