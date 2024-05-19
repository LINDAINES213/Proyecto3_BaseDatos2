import os
import json
import pandas as pd


class HFileManager:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.tables = {}  # Diccionario para almacenar las tablas cargadas

    def list(self):
        table_files = [f for f in os.listdir(self.data_dir) if f.endswith('.json')]
        table_names = [os.path.splitext(f)[0] for f in table_files]
        return table_names

    def count(self, table_name):
        table = self.load_table(table_name)
        if table is None:
            raise ValueError(f"La tabla '{table_name}' no existe.")

        row_count = table.data.shape[0]
        return row_count
    
    def load_table(self, table_name):
        if table_name in self.tables:
            return self.tables[table_name]

        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        if not os.path.exists(table_file):
            return None

        with open(table_file, 'r') as file:
            data = json.load(file)

        column_families = list(data.keys())  # Obtenemos las column families del archivo JSON
        table = Table(table_name, column_families)
        table.load_from_json(data)
        self.tables[table_name] = table
        return table


class Table:
    def __init__(self, name, column_families, avalaibility=True):
        self.name = name
        self.column_families = column_families
        self.data = pd.DataFrame()
        self.avalaibility = avalaibility 

    def load_from_json(self, data):
        """
        Carga los datos de la tabla desde un diccionario simulando el formato de HFiles.
        """
        # Creamos un DataFrame directamente desde el diccionario
        self.data = pd.DataFrame.from_dict(data, orient='index')

    def save_to_json(self, json_file):
        """
        Guarda los datos de la tabla en un archivo JSON.
        """
        # Convertimos el DataFrame a un diccionario y lo guardamos en un archivo JSON
        data = self.data.to_dict(orient='index')
        with open(json_file, 'w') as file:
            json.dump(data, file, indent=2)

    def get(self, row_key, column_family=None, column=None):
        """
        Obtiene los datos de una fila específica o de una columna en particular.
        """
        if self.data.empty:
            return "No hay datos cargados en la tabla."

        try:
            row_data = self.data.loc[row_key]
        except KeyError:
            return f"La fila con clave '{row_key}' no existe en la tabla."

        if column_family and column:
            col_key = f"{column_family}:{column}"
            if col_key not in row_data.index:
                return f"La columna '{col_key}' no existe en la fila '{row_key}'."
            return row_data[col_key]

        elif column_family:
            filtered_data = row_data.filter(like=f"{column_family}:", axis=0)
            if filtered_data.empty:
                return f"No hay columnas en la familia '{column_family}' para la fila '{row_key}'."
            return filtered_data.to_dict()

        else:
            return row_data.to_dict()

    def scan(self, start_row=None, stop_row=None):
        """
        Escanea y muestra los datos de la tabla dentro de un rango específico de filas.
        """
        if self.data.empty:
            return "No hay datos cargados en la tabla."

        if start_row and stop_row:
            filtered_data = self.data.loc[start_row:stop_row]
        elif start_row:
            filtered_data = self.data.loc[start_row:]
        elif stop_row:
            filtered_data = self.data.loc[:stop_row]
        else:
            filtered_data = self.data

        if filtered_data.empty:
            return "No se encontraron filas en el rango especificado."

        return filtered_data.to_dict()
