import os
import json

def ordenar_datos_por_row_key(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)

        if 'data' in data:
            data['data'] = sorted(data['data'], key=lambda x: x['row_key'])
            with open(file_path, 'w') as file:
                json.dump(data, file, indent=2)
            #print(f"Datos ordenados en {file_path}")
        else:
            print(f"No se encontró la clave 'data' en {file_path}")
    except Exception as e:
        print(f"Error al ordenar los datos en {file_path}: {e}")

def ordenar_todos_los_archivos_en_carpeta(carpeta):
    for filename in os.listdir(carpeta):
        if filename.endswith('.json'):
            file_path = os.path.join(carpeta, filename)
            ordenar_datos_por_row_key(file_path)

if __name__ == '__main__':
    carpeta = './datos'
    ordenar_todos_los_archivos_en_carpeta(carpeta)
