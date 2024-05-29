from collections import defaultdict
from itertools import zip_longest
import time
import pandas as pd
import json
import math
import os
import sys
from tabulate import tabulate


def convert_value(value):
    # Primero, intenta convertir a booleano
    if value.lower() in ('true', 'false'):
        return value.lower() == 'true'

    # Intenta convertir a entero
    try:
        return int(value)
    except ValueError:
        pass

    # Intenta convertir a flotante
    try:
        return float(value)
    except ValueError:
        pass

    # Si no se puede convertir, devuelve el valor original
    return value


class Table:
    def __init__(self, name, column_families):
        self.name = name
        self.column_families = column_families
        self.disabled = False
        self.data = []

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
            raise ValueError(
                "El nombre de la tabla no coincide con el archivo JSON.")

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
        self.stats = data.get('stats', {})

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
                        if math.isnan(value):
                            value = 'NaN'
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
            raise ValueError(
                "El nombre de la columna debe estar en el formato 'familia:columna'.")

        family, col_name = col_family_col_name.split(':')

        # Verificar si la familia de columnas existe y obtener max_versions
        col_family = next(
            (cf for cf in self.column_families if cf['name'] == family), None)
        if not col_family:
            raise ValueError(f"La familia de columnas '{
                             family}' no existe en la tabla.")
        max_versions = col_family.get('max_versions', 1)

        # Si la fila no existe, crearla
        if row_key not in self.data.index:
            self.data.loc[row_key] = pd.Series()

        # Actualizar o insertar el valor en la columna correspondiente
        col_key = f"{family}:{col_name}"
        if col_key not in self.data.columns:
            self.data[col_key] = None

        # Verificar si el valor es una lista
        if isinstance(self.data.at[row_key, col_key], list):
            # Si es una lista, agregar el nuevo valor a la lista existente
            self.data.at[row_key, col_key].insert(0, value)
            # Si el número de versiones excede max_versions, eliminar el último elemento
            max_versions = col_family.get('max_versions', 1)
            if len(self.data.at[row_key, col_key]) > max_versions:
                self.data.at[row_key, col_key] = self.data.at[row_key,
                                                              col_key][:max_versions]
        else:
            # Si no es una lista, continuar con la lógica original para verificar si es NaN o None
            if pd.isna(self.data.at[row_key, col_key]) or self.data.at[row_key, col_key] is None:
                # Si el valor es NaN o None, crear una nueva lista con el valor
                self.data.at[row_key, col_key] = [value]
            else:
                # Si el valor no es NaN ni None, continuar con la lógica original de actualización
                if isinstance(self.data.at[row_key, col_key], str):
                    # Si es una cadena, convertirla en una lista con el nuevo valor
                    self.data.at[row_key, col_key] = [value]
                else:
                    # Si ya es una lista, agregar el nuevo valor a la lista existente
                    self.data.at[row_key, col_key].insert(0, value)
                    # Si el número de versiones excede max_versions, eliminar el último elemento
                    if len(self.data.at[row_key, col_key]) > max_versions:
                        self.data.at[row_key, col_key] = self.data.at[row_key,
                                                                      col_key][:max_versions]

        # Guardar los cambios en el archivo JSON
        self.save_to_json(f'{self.name}')

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

        if self.data.empty or len(self.data) == 0:
            return "No hay datos cargados en la tabla."

        try:
            row_data = self.data.loc[row_key]
        except KeyError:
            return f"La fila con clave '{row_key}' no existe en la tabla."

        if column_family and column:
            filtered_data = row_data[row_data.index.str.contains(
                f"^{column_family}:{column}:")]
            if filtered_data.empty:
                return f"La columna '{column_family}:{column}' no existe en la fila '{row_key}'."
            else:
                return filtered_data.to_string(header=False)

        elif column_family:
            filtered_data = row_data[row_data.index.str.startswith(
                f"{column_family}:")]
            return filtered_data.to_string(header=False)

        else:
            table = defaultdict(list)
            for col, value in row_data.items():
                table[col].append(
                    value[0] if isinstance(value, list) else value)

            headers = list(table.keys())
            rows = [list(values)
                    for values in zip_longest(*table.values(), fillvalue='')]

            return tabulate(rows, headers=headers, tablefmt="grid")

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
                if isinstance(timestamps, float) and math.isnan(timestamps):
                    # Manejar el caso de nan
                    processed_row[col_name] = "NaN"
                else:
                    # Proceder con el acceso a los elementos de timestamps
                    processed_row[col_name] = timestamps[0]
            processed_data.append(processed_row)

        formatted_data = pd.DataFrame(processed_data)

        # formatted_data = filtered_data.reset_index().rename(columns={'index': 'row_key'})
        formatted_table = tabulate(
            formatted_data, headers='keys', tablefmt='grid')
        return formatted_table

    def initialize_empty_table(self, json_file=None):
        """
        Inicializa la tabla con una estructura vacía de datos.
        """
        data = {
            'table_name': self.name,
            'column_families': self.column_families,
            'data': [],
            'disabled': self.disabled
        }

        row_key_base = 'row_key'
        column_qualifier_base = 'column_qualifier'
        row_key_counter = 1
        column_qualifier_counter = 1

        for family in self.column_families:
            row_key = f"{row_key_base}{row_key_counter}"
            column_qualifier = f"{column_qualifier_base}{
                column_qualifier_counter}"

            data['data'].append({
                'row_key': row_key,
                'columns': family['name'],
                'timestamps': []
            })

    # Incrementar los contadores para la siguiente iteración
        row_key_counter += 1
        column_qualifier_counter += 1

        # Guardar el objeto JSON en un archivo
        with open('./datos/' + json_file + '.json', 'w') as file:
            json.dump(data, file, indent=2)

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

    def is_enabled(self):
        """
        Verifica si la tabla está habilitada.
        """
        return self.disabled

    def drop(self):
        """
        Elimina la tabla en memoria.
        """
        self.data = pd.DataFrame()
        self.disabled = False
        self.column_families = []
        return f"La tabla '{self.name}' ha sido eliminada en memoria."

    def delete(self, row_key, col_family_col_name=None, timestamp=None):
        """
        Elimina una celda específica, una columna completa o una fila completa de la tabla.

        Args:
            row_key (str): La clave de la fila a eliminar.
            col_family_col_name (str, optional): El nombre de la columna en el formato 'familia:columna'. Si no se proporciona, se eliminará la fila completa.
            timestamp (int, optional): El timestamp de la celda a eliminar. Si no se proporciona, se eliminará la columna completa.
        """
        if row_key not in self.data.index:
            return f"La fila con clave '{row_key}' no existe en la tabla."

        if col_family_col_name:
            if ':' not in col_family_col_name:
                raise ValueError(
                    "El nombre de la columna debe estar en el formato 'familia:columna'.")
            family, col_name = col_family_col_name.split(':')
            col_key = f"{family}:{col_name}"

            if col_key not in self.data.columns:
                return f"La columna '{col_family_col_name}' no existe en la fila '{row_key}'."

            if timestamp is not None:
                # Asegurarse de que la columna contiene una lista de timestamps
                timestamp = convert_value(timestamp)
                if isinstance(self.data.at[row_key, col_key], list):
                    if timestamp in self.data.at[row_key, col_key]:
                        self.data.at[row_key, col_key].remove(timestamp)
                        # Eliminar la columna si la lista está vacía
                        if not self.data.at[row_key, col_key]:
                            self.data.at[row_key, col_key] = float('nan')
                    else:
                        return f"El timestamp '{timestamp}' no existe en la columna '{col_family_col_name}' de la fila '{row_key}'."
                else:
                    return f"La columna '{col_family_col_name}' no contiene múltiples versiones."
            else:
                self.data.at[row_key, col_key] = float('nan')
        else:
            self.data.drop(index=row_key, inplace=True)

        self.save_to_json(self.name)
        return f"Eliminación realizada correctamente."

    def deleteAll(self, row_key):
        """
        Elimina todas las celdas en una fila específica de la tabla.

        Args:
            row_key (str): La clave de la fila a eliminar.
        """
        if row_key not in self.data.index:
            return f"La fila con clave '{row_key}' no existe en la tabla."

        self.data.drop(index=row_key, inplace=True)
        self.save_to_json(self.name)
        return f"Toda la fila con clave '{row_key}' ha sido eliminada de la tabla."

    def truncate(self):
        """
        Trunca (vacía) la tabla deshabilitándola, eliminándola y recreándola con la misma estructura.
        """
        # Deshabilitar la tabla
        # Deshabilitar la tabla
        inicio = time.time()
        print("Disabling Table...")
        self.disable()
        # Guardar la estructura de la tabla
        column_families = self.column_families

        # Eliminar la tabla
        print("Dropping Table...")
        self.drop()

        # Volver a crear la tabla con la misma estructura
        print("Recreating Table...")
        self.column_families = column_families
        self.data = pd.DataFrame(
            columns=[f"{cf['name']}:{col}" for cf in column_families for col in ['column']])
        self.save_to_json(self.name)
        fin = time.time()
        return f"Todos los datos de la tabla '{self.name}' han sido eliminados y la tabla ha sido recreada en {round(fin - inicio, 3)} seg"

    def create_table(name, column_families):
        """S
        Crea una nueva tabla y guarda su estructura en un archivo JSON.

        Args:
            name (str): El nombre de la tabla.
            column_families (list): Una lista de familias de columnas.
        """
        table = Table(name, column_families)
        table.save_to_json(name)
        return table

    def insert_many(self, row_key, columns):
        # Busca la fila con la clave row_key
        row_index = self.data[self.data['row_key'] == row_key].index

        if not row_index.empty:
            # Actualiza los datos si la fila existe
            for family_column, value in columns:
                family, column = family_column.split(':')
                self.data.at[row_index, 'columns'].append({
                    'family': family,
                    'column': column,
                    'timestamps': [value]
                })
        else:
            # Añade una nueva fila si no existe
            new_row = {'row_key': row_key, 'columns': []}
            for family_column, value in columns:
                family, column = family_column.split(':')
                new_row['columns'].append({
                    'family': family,
                    'column': column,
                    'timestamps': [value]
                })
            self.data = self.data.append(new_row, ignore_index=True)

        # Guarda los cambios en el archivo JSON
        self.save_to_json(self.name)
