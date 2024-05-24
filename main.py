import argparse
from tables import Table
from HFileManager import HFileManager
import sys
import json

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
    #parser.add_argument('tabla', type=str, help='Nombre de la tabla')
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
    # Analizar los argumentos
    args = parser.parse_args()

    file_manager = HFileManager('./datos',)

    # Ejecutar el comando correspondiente
    if args.comando == 'get':
        if not args.tabla or not args.row_key:
            parser.error(
                'Debe proporcionar el nombre de la tabla y la clave de la fila')

        # Asume que las familias de columnas se cargan desde el archivo JSON
        tabla = Table(args.tabla, [])
        tabla.load_from_json(args.tabla)
        if tabla.disabled:
            print("No se pueden realizar acciones sobre esta tabla, está deshabilitada")
        else:
            output = tabla.get(args.row_key, args.column_family, args.column)
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
            print(f"La tabla '{args.put_args[0]}' ya se encuentra deshabilitada.")

    elif args.comando == 'list':
        tables = file_manager.list()
        print("\nTablas existentes:")
        for table in tables:
            print(f"- {table}")
        print()

    elif args.comando == 'count':
        if not args.tabla:
            parser.error('Debe proporcionar el nombre de la tabla')

        try:
            row_count, check = file_manager.count(args.tabla)
            if check:
                print(row_count)
            else:
                print(f"""\nNúmero de filas en la tabla '{
                      args.tabla}': {row_count}\n""")
        except ValueError as e:
            print(f"Error: {e}")

    elif args.comando == 'describe':
        if not args.tabla:
            parser.error('Debe proporcionar el nombre de la tabla')
        file_manager.describe(args.tabla, args.tabla)

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
        if not args.tabla or not args.row_key:
            parser.error('Debe proporcionar el nombre de la tabla y la clave de la fila')

        tabla = Table(args.tabla, [])
        tabla.load_from_json(args.tabla)
        result = tabla.delete(args.row_key, args.column, args.timestamp)
        print(result)

    elif args.comando == 'deleteAll':
        if not args.tabla or not args.row_key:
            parser.error('Debe proporcionar el nombre de la tabla y la clave de la fila')

        tabla = Table(args.tabla, [])
        tabla.load_from_json(args.tabla)
        result = tabla.deleteAll(args.row_key)
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
        tabla = Table(args.tabla, [])

        if not args.tabla:
            parser.error('Debe proporcionar el nombre de la tabla')

        tabla.load_from_json(args.tabla)
        result = tabla.truncate()
        print(result)

    else:
        parser.error(f'Comando "{args.comando}" no reconocido')


if __name__ == '__main__':
    main()