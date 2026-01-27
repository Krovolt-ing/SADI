import requests
import pandas as pd
import os
from datetime import datetime
import pytz

# Configuración
FILE_NAME = "sadi_historico.csv"
URL_API = "https://api.cammesa.com/demanda-svc/demanda/ObtieneDemandaYTemperaturaRegion?id_region=1002"

def obtener_datos():
    try:
        # 1. Traer datos de CAMMESA
        res = requests.get(URL_API, timeout=10)
        data = res.json()
        df_raw = pd.DataFrame(data)
        
        # 2. Obtener el último dato válido
        ultimo = df_raw.dropna(subset=['demHoy']).iloc[-1]
        
        # 3. Preparar el dato
        # Convertimos fecha a string
        fecha_str = pd.to_datetime(ultimo['fecha']).strftime("%Y-%m-%d %H:%M:%S")
        
        nuevo_dato = {
            "fecha": fecha_str,
            "demanda": int(ultimo['demHoy']),
            "prevista": int(ultimo.get('demPrevista', 0) or 0),
            "temperatura": float(ultimo['tempHoy'])
        }
        return nuevo_dato
    except Exception as e:
        print(f"Error capturando datos: {e}")
        return None

def actualizar_csv():
    dato = obtener_datos()
    if not dato:
        return

    # Si el archivo no existe, crearlo con headers
    if not os.path.isfile(FILE_NAME):
        df = pd.DataFrame([dato])
        df.to_csv(FILE_NAME, index=False)
        print("Archivo creado y dato guardado.")
    else:
        # Si existe, cargar, verificar duplicados y agregar
        df = pd.read_csv(FILE_NAME)
        
        # Chequeamos si la fecha ya existe para no duplicar
        if dato['fecha'] not in df['fecha'].values:
            nuevo_df = pd.DataFrame([dato])
            df = pd.concat([df, nuevo_df], ignore_index=True)
            df.to_csv(FILE_NAME, index=False)
            print(f"Nuevo dato guardado: {dato['fecha']}")
        else:
            print("El dato ya existía, no se duplicó.")

if __name__ == "__main__":
    actualizar_csv()
