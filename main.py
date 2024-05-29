import argparse
from tables import Table, convert_value
from HFileManager import HFileManager
from sortAllFiles import ordenar_todos_los_archivos_en_carpeta, ordenar_datos_por_row_key
import sys
import json
import random
valid_keys = {
    "name",
    "max_versions",
    "compression",
    "in_memory",
    "bloom_filter",
    "ttl",
    "blocksize",
    "blockcache"
}


def create_and_convert_dict(pairs, parser):
    # Crear el diccionario con los pares
    details = dict(zip(pairs[::2], pairs[1::2]))

    # Convertir los valores que sean texto pero pueden ser números o booleanos
    for key, value in details.items():
        details[key] = convert_value(value)

    for key in details:
        if key not in valid_keys:
            parser.error(f"La clave '{key}' no es válida.")

    return details


def main():
    # Crear el parser de argumentos
    parser = argparse.ArgumentParser(description='Simulador de HBase')
    parser.add_argument('comando', type=str, help='Comando a ejecutar')
    parser.add_argument('--tabla', type=str, help='Nombre de la tabla')
    # parser.add_argument('tabla', type=str, help='Nombre de la tabla')
    parser.add_argument('--archivo', type=str, help='Ruta del archivo JSON')
    parser.add_argument('--row_key', type=str, help='Clave de la fila')
    parser.add_argument('--column_family', type=str,
                        help='Familia de columnas')
    parser.add_argument('--column', type=str, help='Nombre de la columna')

    # Scan de tablas segun rango de filas
    parser.add_argument('--start_row', type=str,
                        help='Clave de la fila de inicio (inclusive)')
    parser.add_argument('--stop_row', type=str,
                        help='Clave de la fila de fin (exclusiva)')

    # Argumentos específicos para el comando 'put'
    parser.add_argument('put_args', nargs='*',
                        help='Argumentos para el comando put')
    parser.add_argument('--add', action='store_true',
                        help='Agregar una nueva column family')
    parser.add_argument('--modify', nargs='+',
                        help='Argumentos de modificación para la columna familia')

    parser.add_argument('--delete', action='store_true',
                        help='Eliminar una column family existente')
    parser.add_argument('--column_families', nargs='+',
                        help='Familias de columnas')
    parser.add_argument('--columnas_y_valores', nargs='+',
                        help='Lista de columnas y valores')

    # Analizar los argumentos
    args = parser.parse_args()

    ordenar_todos_los_archivos_en_carpeta("./datos")

    file_manager = HFileManager('./datos',)

    # Ejecutar el comando correspondiente
    if args.comando == 'get':
        if not args.put_args[0] or not args.put_args[1]:
            parser.error(
                'Debe proporcionar el nombre de la tabla y la clave de la fila')

        # Asume que las familias de columnas se cargan desde el archivo JSON
        tabla = Table(args.put_args[0], [])
        tabla.load_from_json(args.put_args[0])
        if tabla.disabled:
            print("No se pueden realizar acciones sobre esta tabla, está deshabilitada")
        else:
            output = tabla.get(args.put_args[1])
            print(output)

    elif args.comando == 'scan':
        if not args.put_args[0]:
            parser.error('Debe proporcionar el nombre de la tabla')

        # Asume que las familias de columnas se cargan desde el archivo JSON
        tabla = Table(args.put_args[0], [])
        tabla.load_from_json(args.put_args[0])
        if tabla.disabled:
            print("No se pueden realizar acciones sobre esta tabla, está deshabilitada")
        else:
            output = tabla.scan(args.start_row, args.stop_row)
            print(output)

    elif args.comando == 'disable':
        tabla = Table(args.put_args[0], [])
        tabla.load_from_json(args.put_args[0])
        if tabla.disabled:
            print(f"La tabla '{args.put_args[0]}' ya se encuentra deshabilitada.")
        else:
            tabla.disable()
            print(f"La tabla '{args.put_args[0]}' ha sido deshabilitada.")

    elif args.comando == 'enable':
        tabla = Table(args.put_args[0], [])
        tabla.load_from_json(args.put_args[0])
        if tabla.disabled:
            tabla.enable()
            print(f"La tabla '{args.put_args[0]}' ha sido habilitada.")
        else:
            print(f"La tabla '{args.put_args[0]}' ya se encuentra habilitada.")

    elif args.comando == 'list':
        tables = file_manager.list()
        print("\nTablas existentes:")
        for table in tables:
            print(f"- {table}")
        print()

    elif args.comando == 'count':
        if not args.put_args[0]:
            parser.error('Debe proporcionar el nombre de la tabla')

        try:
            row_count, check = file_manager.count(args.put_args[0])
            if check:
                print(row_count)
            else:
                print(f"""\nNúmero de filas en la tabla '{args.put_args[0]}': {row_count}\n""")
        except ValueError as e:
            print(f"Error: {e}")

    elif args.comando == 'describe':
        if not args.put_args[0]:
            parser.error('Debe proporcionar el nombre de la tabla')
        file_manager.describe(args.put_args[0], args.put_args[0])

    elif args.comando == 'is_disabled':
        tabla = Table(args.put_args[0], [])
        tabla.load_from_json(args.put_args[0])
        if tabla:
            if tabla.is_disabled():
                print(f"La tabla '{args.put_args[0]}' está deshabilitada.")
            else:
                print(f"La tabla '{args.put_args[0]}' está habilitada.")
        else:
            print(f"La tabla '{args.put_args[0]}' no existe.")

    elif args.comando == 'is_enabled':
        tabla = Table(args.put_args[0], [])
        tabla.load_from_json(args.put_args[0])
        if tabla:
            if tabla.is_enabled():
                print(f"La tabla '{args.put_args[0]}' está deshabilitada.")
            else:
                print(f"La tabla '{args.put_args[0]}' está habilitada.")
        else:
            print(f"La tabla '{args.put_args[0]}' no existe.")

    elif args.comando == 'put':
        if len(args.put_args) < 4:
            parser.error(
                'Debe proporcionar los argumentos: <tabla> <row_key> <col_family:col_name> <value>')
        tabla = Table(args.put_args[0], [])
        tabla.load_from_json(args.put_args[0])
        row_key = args.put_args[1]
        col_family_col_name = args.put_args[2]
        value = ' '.join(args.put_args[3:])
        if tabla.disabled:
            print("No se pueden realizar acciones sobre esta tabla, está deshabilitada")
        else:
            tabla.put(row_key, col_family_col_name, value)
            tabla.save_to_json(args.put_args[0])
            ordenar_datos_por_row_key(f"./datos/{args.put_args[0]}.json")
            print("Datos insertados correctamente.")
            print(tabla.scan(None,None))

    # Ejecutar el comando correspondiente
    elif args.comando == 'drop':
        if not args.put_args[0]:
            parser.error('Debe proporcionar el nombre de la tabla')
        result = file_manager.drop_table(args.put_args[0])
        print(result)

    elif args.comando == 'drop_all':
        result = file_manager.drop_all()
        print(result)

    elif args.comando == 'delete':
        if not args.put_args[0] or not args.put_args[1]:
            parser.error(
                'Debe proporcionar el nombre de la tabla y la clave de la fila')

        tabla = Table(args.put_args[0], [])
        tabla.load_from_json(args.put_args[0])
        if tabla.disabled:
            print("No se pueden realizar acciones sobre esta tabla, está deshabilitada")
        else:
            
            if len(args.put_args) > 3:
                result = tabla.delete(
                    args.put_args[1], args.put_args[2], args.put_args[3])
            elif len(args.put_args) > 2:
                result = tabla.delete(args.put_args[1], args.put_args[2])
            else:
                result = tabla.delete(args.put_args[1])
            print(result)

    elif args.comando == 'deleteAll':
        if not args.put_args[0] or not args.put_args[1]:
            parser.error(
                'Debe proporcionar el nombre de la tabla y la clave de la fila')

        tabla = Table(args.put_args[0], [])
        tabla.load_from_json(args.put_args[0])
        if tabla.disabled:
            print("No se pueden realizar acciones sobre esta tabla, está deshabilitada")
        else:
            result = tabla.deleteAll(args.put_args[1])
            print(result)

    elif args.comando == 'alter':
        if not args.put_args[0] or not args.put_args[1]:
            parser.error(
                'Debe proporcionar los argumentos: <tabla> <column_family>')

        table = file_manager.load_table(args.put_args[0])
        if not table:
            parser.error(f"La tabla '{args.put_args[0]}' no existe.")

        if args.add:
            existing_families = [cf['name'] for cf in table.column_families]
            if args.put_args[1] in existing_families:
                print(f"La columna familia '{args.put_args[1]}' ya existe en la tabla '{args.put_args[0]}'.")
            else:
                new_family = {
                    "name": args.put_args[1],
                    "max_versions": 3,
                    "compression": "NONE",
                    "in_memory": False,
                    "bloom_filter": "ROW",
                    "ttl": 604800,
                    "blocksize": 65536,
                    "blockcache": True
                }
                table.column_families.append(new_family)
                table.save_to_json(args.put_args[0])
                print(f"La columna familia '{args.put_args[1]}' ha sido agregada a la tabla '{args.put_args[0]}'.")

        elif args.modify:
            details = create_and_convert_dict(args.modify, parser)
            for family in table.column_families:
                if family['name'] == args.put_args[1]:
                    family.update(details)
                    table.save_to_json(args.put_args[0])
                    print(f"La columna familia '{args.put_args[1]}' ha sido modificada en la tabla '{args.put_args[0]}'.")
                    return
            print(f"La columna familia '{args.put_args[1]}' no se encontró en la tabla '{args.put_args[0]}'.")

        elif args.delete:
            table.column_families = [
                cf for cf in table.column_families if cf['name'] != args.put_args[1]]
            table.save_to_json(args.put_args[0])
            print(f"La columna familia '{args.put_args[1]}' ha sido eliminada de la tabla '{args.put_args[0]}'.")

        else:
            print("Debe especificar una acción: --add, --modify <detalles> o --delete.")

    elif args.comando == 'truncate':
        if not args.put_args[0]:
            parser.error('Debe proporcionar el nombre de la tabla')

        tabla = Table(args.put_args[0], [])

        tabla.load_from_json(args.put_args[0])
        result = tabla.truncate()
        print(result)

    elif args.comando == 'create':
        if not args.put_args[0] or not args.put_args[1]:
            parser.error(
                'Debe proporcionar el nombre de la tabla y al menos una familia de columnas.')
          # Crear la tabla con las familias de columnas especificadas
        column_families = [{
            'name': name,
            'max_versions': random.randint(0, 10),
            'compression': 'NONE',
            'in_memory': False,
            'bloom_filter': 'ROW',
            'ttl': random.randint(3600, 604800),
            'blocksize': random.randint(1024, 65536),
            'blockcache': False
        } for name in args.put_args[1]]
        data = []
        for i in range(1, 6):  # Generar datos para 5 filas de ejemplo
            row_key = f"row_{i}"
            columns = []
            for family in args.put_args[1]:
                # Generar datos de ejemplo para cada columna de la familia
                column_data = {
                    "family": family,
                    "columns": "nombre",  # Puedes ajustar el nombre de la columna si es necesario
                    "timestamps": [f"Ejemplo de dato para {family}"]
                }
                columns.append(column_data)
            row_data = {
                "row_key": row_key,
                "columns": columns
            }
            data.append(row_data)
        table = Table(args.put_args[0], column_families)

        # Guardar la tabla en formato JSON
        json_file = f"{args.put_args[0]}"
        table.initialize_empty_table(json_file)
        print(f"Tabla '{args.put_args[0]}' creada exitosamente con las siguientes familias de columnas: {', '.join(args.put_args[1:])}")

    elif args.comando == 'insert_many':
        if not args.tabla or not args.row_key or not args.columnas_y_valores:
            parser.error(
                "Debe proporcionar el nombre de la tabla, la clave de la fila y la lista de columnas y valores en formato JSON.")

        try:
            # Intenta dividir las columnas y valores utilizando ':'
            columnas_y_valores = [pair.split(':')
                                  for pair in args.columnas_y_valores]
        except ValueError:
            # Si hay un error al dividir, imprime un mensaje de error y finaliza el programa
            print("Error: Formato no válido para columnas y valores.")
            return

        # Continúa con la ejecución del comando insert_many
        table = Table(args.tabla, [])
        table.load_from_json(args.tabla)
        table.insert_many(args.row_key, columnas_y_valores)
        print(f"Valores insertados en la fila '{args.row_key}' de la tabla '{args.tabla}'.")
        print(f"Tabla '{args.put_args[0]}' creada exitosamente con las siguientes familias de columnas: {', '.join(args.put_args[1:])}")
    else:
        parser.error(f'Comando "{args.comando}" no reconocido')


if __name__ == '__main__':
    main()