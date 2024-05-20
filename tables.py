import pandas as pd
import json
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

        with open('./datos/' + json_file  + ".json", 'r') as file:
            data = json.load(file)

        # Verificar que el nombre de la tabla coincida
        if data['table_name'] != self.name:
            raise ValueError("El nombre de la tabla no coincide con el archivo JSON.")

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

    def save_to_json(self, json_file):
        """
        Guarda los datos de la tabla en un archivo JSON.
        """
        data = {
            'table_name': self.name,
            'column_families': self.column_families,
            'data': []
        }

        for row_key, row in self.data.iterrows():
            row_data = {
                'row_key': row_key,
                'columns': []
            }
            for col_key, value in row.items():
                if col_key != 'row_key':
                    family, col_name = col_key.split(':')
                    column = {
                        'family': family,
                        'column': col_name,
                        'timestamps': value.timestamps
                    }
                    row_data['columns'].append(column)
            data['data'].append(row_data)

        with open(json_file, 'w') as file:
            json.dump(data, file, indent=2)

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