import argparse
from tables import Table
from HFileManager import HFileManager


def main():
    # Crear el parser de argumentos
    parser = argparse.ArgumentParser(description='Simulador de HBase')
    parser.add_argument('comando', type=str, help='Comando a ejecutar')
    parser.add_argument('--tabla', type=str, help='Nombre de la tabla')
    parser.add_argument('--archivo', type=str, help='Ruta del archivo JSON')
    parser.add_argument('--row_key', type=str, help='Clave de la fila')
    parser.add_argument('--column_family', type=str, help='Familia de columnas')
    parser.add_argument('--column', type=str, help='Nombre de la columna')

    # Scan de tablas segun rango de filas
    parser.add_argument('--start_row', type=str, help='Clave de la fila de inicio (inclusive)')
    parser.add_argument('--stop_row', type=str, help='Clave de la fila de fin (exclusiva)')

    # Argumentos específicos para el comando 'put'
    parser.add_argument('put_args', nargs='*', help='Argumentos para el comando put')

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
        if not args.tabla:
            parser.error('Debe proporcionar el nombre de la tabla')

        # Asume que las familias de columnas se cargan desde el archivo JSON
        tabla = Table(args.tabla, [])
        tabla.load_from_json(args.tabla)
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
                print(f"""\nNúmero de filas en la tabla '{args.tabla}': {row_count}\n""")
        except ValueError as e:
            print(f"Error: {e}")

    elif args.comando == 'describe':
        if not args.tabla:
            parser.error('Debe proporcionar el nombre de la tabla')
        if tabla.disabled:
            print("No se pueden realizar acciones sobre esta tabla, está deshabilitada")
        else:
            file_manager.describe(args.tabla, args.tabla)

    elif args.comando == 'put':
        if len(args.put_args) < 4:
            parser.error('Debe proporcionar los argumentos: <tabla> <row_key> <col_family:col_name> <value>')
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

    else:
        parser.error(f'Comando "{args.comando}" no reconocido')


if __name__ == '__main__':
    main()

'''    # Crear el parser de argumentos
    parser = argparse.ArgumentParser(description='Simulador de HBase')
    parser.add_argument('comando', type=str, help='Comando a ejecutar')

    # Argumentos para el comando 'leer'
    parser.add_argument('--tabla', type=str, help='Nombre de la tabla')
    parser.add_argument('--archivo', type=str, help='Ruta del archivo JSON')

    # Argumentos para el comando 'get'
    parser.add_argument('--row_key', type=str, help='Clave de la fila')
    parser.add_argument('--column_family', type=str, help='Familia de columnas')
    parser.add_argument('--column', type=str, help='Nombre de la columna')

    # Argumentos para el comando 'scan'
    parser.add_argument('--start_row', type=str, help='Clave de la fila de inicio (inclusive)')
    parser.add_argument('--stop_row', type=str, help='Clave de la fila de fin (exclusiva)')

    # Analizar los argumentos
    args = parser.parse_args()

    # Ejecutar el comando correspondiente
    if args.comando == 'leer':
        if not args.tabla or not args.archivo:
            parser.error('Debe proporcionar el nombre de la tabla y la ruta del archivo JSON')

        tabla = Table(args.tabla)
        tabla.load_from_json(args.archivo)
        print(tabla.data)

    elif args.comando == 'get':
        if not args.tabla or not args.archivo or not args.row_key:
            parser.error('Debe proporcionar el nombre de la tabla, la ruta del archivo JSON y la clave de la fila')

        tabla = Table(args.tabla)
        tabla.load_from_json(args.archivo)
        output = tabla.get(args.row_key, args.column_family, args.column)
        print(output)

    elif args.comando == 'scan':
        if not args.tabla or not args.archivo:
            parser.error('Debe proporcionar el nombre de la tabla y la ruta del archivo JSON')

        tabla = Table(args.tabla)
        tabla.load_from_json(args.archivo)
        output = tabla.scan(args.start_row, args.stop_row)
        print(output)

    else:
        parser.error(f'Comando "{args.comando}" no reconocido')

if __name__ == '__main__':
    main()'''
