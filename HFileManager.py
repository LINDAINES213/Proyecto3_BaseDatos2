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
            print(f"Nombre de la tabla: {tabla}")
            print(f"Archivo JSON: {archivo}")
            print("Familias de columnas:")
            for family in table.column_families:
                if isinstance(family, dict):
                    name = family.get('name', '')
                    max_versions = family.get('max_versions', '')
                    compression = family.get('compression', '')
                    in_memory = family.get('in_memory', '')
                    bloom_filter = family.get('bloom_filter', '')
                    print(f"- {name}")
                    print(f"  - Máximas versiones: {max_versions}")
                    print(f"  - Compresión: {compression}")
                    print(f"  - En memoria: {in_memory}")
                    print(f"  - Bloom: {bloom_filter}")
                    print(
                        f"  - TTL: {family.get('ttl', 'Desconocido')} segundos")
                    print(
                        f"  - Blocksize: {family.get('blocksize', 'Desconocido')} bytes")
                    print(f"  - Blockcache: {'Habilitada' if family.get(
                        'blockcache', False) else 'Deshabilitada'}")
                else:
                    print(f"- {family}")

            # Mostrar estadísticas
            stats = table.stats if hasattr(table, 'stats') else None
            if stats:
                total_rows = stats.get('total_rows', 'Desconocido')
                total_columns = stats.get('total_columns', 'Desconocido')
                max_column_family_size = stats.get(
                    'max_column_family_size', {})

                print(f"\nEstadísticas:")
                print(f"Rows: {total_rows}")
                print(f"Columns: {total_columns}")
                for family, size in max_column_family_size.items():
                    print(f"- {family}: {size}")
        else:
            print(f"No se encontró la tabla '{tabla}'.")

    def alter_table(self, table_name, column_family):
        table = self.load_table(table_name)
        if table is None:
            print(f"La tabla '{table_name}' no existe.")
            return

        # Verificar si la nueva column family ya existe
        existing_families = [cf['name'] for cf in table.column_families]
        if column_family in existing_families:
            print(f"La columna familia '{
                  column_family}' ya existe en la tabla '{table_name}'.")
            return

        # Agregar la nueva column family
        new_family = {
            "name": column_family,
            "max_versions": 3,
            "compression": "NONE",
            "in_memory": False,
            "bloom_filter": "ROW",
            "ttl": 604800,
            "blocksize": 65536,
            "blockcache": True
            # Añade otros atributos según sea necesario
        }
        table.column_families.append(new_family)

        # Guardar los cambios en el archivo JSON
        table.save_to_json(table_name)
        print(f"La columna familia '{
              column_family}' ha sido agregada a la tabla '{table_name}'.")
