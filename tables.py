import pandas as pd
import json
import math
import os
import sys
from tabulate import tabulate

class Table:
    def __init__(self, name, column_families):
        self.name = name
        self.column_families = column_families
        self.data = pd.DataFrame()

    def load_from_json(self, json_file):
        """
        Carga los datos de la tabla desde un archivo JSON.
        """
        json_path = os.path.join('./datos', f"{json_file}.json")
        if not os.path.exists(json_path):
            print(f"Error: La tabla '{json_file}' no existe.")
            sys.exit(1)
            
            
        with open(json_path, 'r') as file:
            data = json.load(file)

        # Verificar que el nombre de la tabla coincida
        if data['table_name'] != self.name:
            raise ValueError("El nombre de la tabla no coincide con el archivo JSON.")

        self.disabled = data['disabled']

        # Crear una lista de diccionarios para el DataFrame
        rows = []
        for row in data['data']:
            row_key = row['row_key']
            columns = row['columns']
            row_dict = {'row_key': row_key}
            for column in columns:
                family = column['family']
                col_name = column['column']
                timestamps = column['timestamps']
                col_key = f"{family}:{col_name}"
                row_dict[col_key] = timestamps
            rows.append(row_dict)

        # Crear el DataFrame
        self.data = pd.DataFrame(rows)
        self.data = self.data.set_index('row_key')
        self.column_families = data['column_families']

    def save_to_json(self, json_file):
        """
        Guarda los datos de la tabla en un archivo JSON.
        """
        data = {
            'table_name': self.name,
            'column_families': self.column_families,
            'data': [],
            'disabled': self.disabled
        }

        for row_key, row in self.data.iterrows():
            row_data = {
                'row_key': row_key,
                'columns': [],

            }
            for col_key, value in row.items():
                if col_key != 'row_key':
                    if not isinstance(value, list) and not isinstance(value, str):
                        print(value)
                        if math.isnan(value): value = 'NaN'
                    family, col_name = col_key.split(':')
                    column = {
                        'family': family,
                        'column': col_name,
                        'timestamps': value
                    }
                    row_data['columns'].append(column)
            data['data'].append(row_data)

        with open('./datos/' + json_file + '.json', 'w') as file:
            json.dump(data, file, indent=2)


    def put(self, row_key, col_family_col_name, value):
        """
        Inserta o actualiza un valor en la tabla y guarda los cambios en un archivo JSON.

        Args:
            row_key (str): La clave de la fila.
            col_family_col_name (str): El nombre de la columna en el formato 'familia:columna'.
            value (str): El valor a insertar.
        """
        if ':' not in col_family_col_name:
            raise ValueError("El nombre de la columna debe estar en el formato 'familia:columna'.")

        family, col_name = col_family_col_name.split(':')

        # Verificar si la familia de columnas existe y obtener max_versions
        col_family = next((cf for cf in self.column_families if cf['name'] == family), None)
        if not col_family:
            raise ValueError(f"La familia de columnas '{family}' no existe en la tabla.")
        max_versions = col_family.get('max_versions', 1)

        # Si la fila no existe, crearla
        if row_key not in self.data.index:
            self.data.loc[row_key] = pd.Series()

        # Actualizar o insertar el valor en la columna correspondiente
        col_key = f"{family}:{col_name}"
        if col_key not in self.data.columns:
            self.data[col_key] = None

        if pd.isna(self.data.at[row_key, col_key]).any():
            self.data.at[row_key, col_key] = [value]
        else:
            if isinstance(self.data.at[row_key, col_key], str):
                self.data.at[row_key, col_key] = [value]
            else:
                self.data.at[row_key, col_key].insert(0, value)
                # Si el número de versiones excede max_versions, eliminar el último elemento
                if len(self.data.at[row_key, col_key]) > max_versions:
                    self.data.at[row_key, col_key] = self.data.at[row_key, col_key][:max_versions]

        # Guardar los cambios en el archivo JSON
        self.save_to_json(f'{self.name}')
        print(self.data)
        
    def get(self, row_key, column_family=None, column=None):
        """
        Obtiene los datos de una fila específica o de una columna en particular.

        Args:
            row_key (str): La clave de la fila.
            column_family (str, optional): La familia de columnas. Si se proporciona, se filtrarán las columnas correspondientes.
            column (str, optional): El nombre de la columna. Si se proporciona junto con column_family, se filtrará la columna específica.

        Returns:
            str: Una representación formateada de los datos solicitados.
        """
        if self.data.empty:
            return "No hay datos cargados en la tabla."

        try:
            row_data = self.data.loc[row_key]
        except KeyError:
            return f"La fila con clave '{row_key}' no existe en la tabla."

        if column_family and column:
            filtered_data = row_data[row_data.index.str.contains(f"^{column_family}:{column}:")]
            if filtered_data.empty:
                return f"La columna '{column_family}:{column}' no existe en la fila '{row_key}'."
            else:
                return filtered_data.to_string(header=False)

        elif column_family:
            filtered_data = row_data[row_data.index.str.startswith(f"{column_family}:")]
            return filtered_data.to_string(header=False)

        else:
            return row_data.to_string(header=False)
        
    def scan(self, start_row=None, stop_row=None):
        """
        Escanea y muestra los datos de la tabla dentro de un rango específico de filas.

        Args:
            start_row (str, optional): La clave de la fila de inicio (inclusive).
            stop_row (str, optional): La clave de la fila de fin (exclusiva).

        Returns:
            str: Una representación tabulada de los datos escaneados.
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

        processed_data = []
        for index, row in filtered_data.iterrows():
            processed_row = {'row_key': index}
            for col_name, timestamps in row.items():
                processed_row[col_name] = timestamps[0]  # Solo tomar el primer elemento
            processed_data.append(processed_row)

        formatted_data = pd.DataFrame(processed_data)

        #formatted_data = filtered_data.reset_index().rename(columns={'index': 'row_key'})
        formatted_table = tabulate(formatted_data, headers='keys', tablefmt='grid')
        return formatted_table
    
    def disable(self):
        """
        Deshabilita la tabla.
        """
        self.disabled = True
        self.save_to_json(self.name)

    def is_disabled(self):
        """
        Verifica si la tabla está deshabilitada.
        """
        return self.disabled

    def enable(self):
        """
        Deshabilita la tabla.
        """
        self.disabled = False
        self.save_to_json(self.name)

    def drop(self):
        """
        Elimina la tabla en memoria.
        """
        self.data = pd.DataFrame()
        self.disabled = False
        self.column_families = []
        return f"La tabla '{self.name}' ha sido eliminada en memoria."
    

'''
    def drop_table(self, table_name):
        """
        Elimina una tabla tanto en memoria como el archivo JSON correspondiente.
        """
        # Eliminar la tabla en memoria si está cargada
        if table_name in self.tables:
            self.tables[table_name].drop()
            del self.tables[table_name]

        # Eliminar el archivo JSON correspondiente
        table_file = os.path.join(self.data_dir, table_name + '.json')
        if os.path.exists(table_file):
            os.remove(table_file)
            return f"La tabla '{table_name}' ha sido eliminada del sistema de archivos y de la memoria."
        else:
            return f"La tabla '{table_name}' no existe en el sistema de archivos."
 '''       

