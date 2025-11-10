import requests
import time
import json
import csv

URL_BASE = "https://openlibrary.org/search.json"
CABECERAS = {"User-Agent": "Proyecto_ETL_Bootcamp/1.0 (Proposito Educativo)"}


def extraer_libros(tema="science_fiction", max_resultados=5000):
    todos_los_libros = []
    desplazamiento = 0
    resultados_por_pagina = 100
    
    print(f"Extrayendo {max_resultados} libros con TODOS los campos...")
    
    while len(todos_los_libros) < max_resultados:
        parametros = {
            "subject": tema,
            "limit": resultados_por_pagina,
            "offset": desplazamiento,
            "fields": "*"  # Obtener TODOS los campos que haiga
        }
        
        try:
            print(f"Solicitando desplazamiento {desplazamiento}...")
            respuesta = requests.get(URL_BASE, params=parametros, headers=CABECERAS, timeout=30)
            respuesta.raise_for_status()
            datos = respuesta.json()
            
            documentos = datos.get("docs", [])
            if not documentos:
                print("No hay más resultados disponibles :(")
                break
            
            todos_los_libros.extend(documentos)
            print(f"Extraídos: {len(todos_los_libros)}/{max_resultados} :D")
            
            desplazamiento += resultados_por_pagina
            
            if len(documentos) < resultados_por_pagina or len(todos_los_libros) >= max_resultados:
                break
            
            # Retraso para que no me bloqueeen
            time.sleep(1)
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print("Límite de peticiones alcanzado (429). Esperando 10 segundos...")
                time.sleep(10)
                continue
            else:
                print(f"Error HTTP: {e}")
                break
        except Exception as e:
            print(f"Error: {e}")
            break
    
    return todos_los_libros[:max_resultados]


def guardar_json(libros, nombre_archivo="libros_raw.json"):
    with open(nombre_archivo, 'w', encoding='utf-8') as f:
        json.dump(libros, f, indent=2, ensure_ascii=False)
    print(f"Guardado: {nombre_archivo}")


def guardar_csv(libros, nombre_archivo="libros_raw.csv"):
    if not libros:
        return
    
    todas_las_claves = set()
    for libro in libros:
        todas_las_claves.update(libro.keys())
    
    with open(nombre_archivo, 'w', encoding='utf-8', newline='') as f:
        escritor = csv.DictWriter(f, fieldnames=sorted(todas_las_claves))
        escritor.writeheader()
        
        for libro in libros:
            fila = {}
            for clave, valor in libro.items():
                if isinstance(valor, list):
                    fila[clave] = "|".join(map(str, valor))
                else:
                    fila[clave] = valor
            escritor.writerow(fila)
    
    print(f"Guardado: {nombre_archivo}")


if __name__ == "__main__":
    datos_libros = extraer_libros(max_resultados=5000)
    
    guardar_json(datos_libros)
    guardar_csv(datos_libros)
    
    print(f"\nTotal de libros: {len(datos_libros)}")