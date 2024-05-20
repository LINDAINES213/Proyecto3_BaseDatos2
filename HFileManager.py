import os
import json
from tables import Table


class HFileManager:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.tables = {}  # Diccionario para almacenar las tablas cargadas

    def list(self):
        table_files = [f for f in os.listdir(
            self.data_dir) if f.endswith('.json')]
        table_names = [os.path.splitext(f)[0] for f in table_files]
        return table_names

    def count(self, table_name):
        table = self.load_table(table_name)
        if table is None:
            raise ValueError(f"La tabla '{table_name}' no existe.")

        if table.disabled:
            return "No se pueden realizar acciones sobre esta tabla, está deshabilitada", True
        else:
            row_count = len(table.data)
            return row_count, False

    def load_table(self, table_name):
        if table_name in self.tables:
            return self.tables[table_name]

        table_file = './datos/' + table_name + '.json'
        if not os.path.exists(table_file):
            return None

        with open(table_file, 'r') as file:
            data = json.load(file)

        column_families = data['column_families']
        table = Table(table_name, column_families)
        table.load_from_json(table_name)
        self.tables[table_name] = table
        return table

    def describe(self, tabla, archivo):
        table = self.load_table(tabla)
        if table:
            if table.disabled:
                print("No se pueden realizar acciones sobre esta tabla, está deshabilitada")
            else:
                print(f"Nombre de la tabla: {tabla}")
                print(f"Archivo JSON: {archivo}")
                print("Data de archivo:")
                for column_families in table.column_families:
                    print(f"- {column_families}")
        else:
            print(f"No se encontró la tabla '{tabla}'.")



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

    def drop_all(self):
        """
        Elimina todas las tablas tanto en memoria como sus archivos JSON correspondientes.
        """
        # Obtener la lista de todas las tablas
        all_tables = self.list()

        # Eliminar todas las tablas en memoria
        for table_name in list(self.tables.keys()):
            self.tables[table_name].drop()
            del self.tables[table_name]

        # Eliminar todos los archivos JSON correspondientes
        for table_name in all_tables:
            table_file = os.path.join(self.data_dir, table_name + '.json')
            if os.path.exists(table_file):
                os.remove(table_file)

        return "Todas las tablas han sido eliminadas del sistema de archivos y de la memoria."
