import requests
import pandas as pd
import os
from datetime import datetime

FILE_NAME = "sadi_historico.csv"
URL_API = "https://api.cammesa.com/demanda-svc/demanda/ObtieneDemandaYTemperaturaRegion?id_region=1002"

def actualizar_csv():
    print(f"Iniciando captura: {datetime.now()}")
    
    # Si el archivo no existe, lo creamos vacío con las columnas para que Git no proteste
    if not os.path.isfile(FILE_NAME):
        pd.DataFrame(columns=["fecha", "demanda", "prevista", "temperatura"]).to_csv(FILE_NAME, index=False)
        print("Archivo base creado.")

    try:
        # Intentamos bajar el dato
        res = requests.get(URL_API, timeout=20)
        data = res.json()
        
        if data:
            df_raw = pd.DataFrame(data)
            df_real = df_raw.dropna(subset=['demHoy'])
            
            if not df_real.empty:
                ultimo = df_real.iloc[-1]
                fecha_str = pd.to_datetime(ultimo['fecha']).strftime("%Y-%m-%d %H:%M:%S")
                
                nuevo_dato = {
                    "fecha": fecha_str,
                    "demanda": int(ultimo['demHoy']),
                    "prevista": int(ultimo.get('demPrevista', 0) or 0),
                    "temperatura": float(ultimo['tempHoy'])
                }

                # Leemos el archivo actual y agregamos el dato
                df = pd.read_csv(FILE_NAME)
                if nuevo_dato['fecha'] not in df['fecha'].values:
                    # Usamos pd.concat en lugar de append (que es viejo)
                    df_nuevo = pd.DataFrame([nuevo_dato])
                    df = pd.concat([df, df_nuevo], ignore_index=True)
                    df.to_csv(FILE_NAME, index=False)
                    print(f"✅ Dato nuevo guardado: {fecha_str}")
                else:
                    print("Dato ya existente. No se duplica.")
            else:
                print("⚠️ No hay datos de 'demHoy' en el JSON.")
        else:
            print("❌ La API devolvió una lista vacía.")
                
    except Exception as e:
        print(f"🔥 Error en la conexión o procesamiento: {e}")

if __name__ == "__main__":
    actualizar_csv()
