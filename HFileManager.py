import os
import json
from tables import Table

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

        row_count = len(table.data)
        return row_count
    
    def load_table(self, table_name):
        if table_name in self.tables:
            return self.tables[table_name]

        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        if not os.path.exists(table_file):
            return None

        with open(table_file, 'r') as file:
            data = json.load(file)

        column_families = data['column_families']
        table = Table(table_name, column_families)
        table.load_from_json(table_file)
        self.tables[table_name] = table
        return table