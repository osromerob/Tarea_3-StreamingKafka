import requests
import time
import json
from kafka import KafkaProducer

# URL de la API OpenSky
URL = "https://opensky-network.org/api/states/all"

def obtener_informacion_opensky():
    try:
        # Hacer una solicitud GET a la API
        response = requests.get(URL)
        # Comprobar si la solicitud fue exitosa
        if response.status_code == 200:
            datos = response.json()
            return datos
        else:
            print(f"Error en la solicitud: {response.status_code}")
            return None
    except Exception as e:
        print(f"Ocurrió un error: {e}")
        return None

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def main():
    while True:
        # Obtener información de OpenSky
        datos = obtener_informacion_opensky()
        if datos:
            # Procesar y mostrar información relevante
            print("Información recibida:")
            for estado in datos.get('states', []):
            	vuelo_info = {
            		"id_ICAO24": estado[0],
            		"callsign": estado[1],
            		"country": estado[2],
            		"longitude": estado[5],
            		"altitude": estado[6]
            	}
            	producer.send('OpenSky_data', value=vuelo_info)
            	print(vuelo_info)
        # Esperar un tiempo antes de la siguiente consulta
        time.sleep(10)  # Consultar cada 60 segundos

if __name__ == "__main__":
    main()
