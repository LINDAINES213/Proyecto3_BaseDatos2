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
    parser.add_argument('--column_family', type=str,
                        help='Familia de columnas')
    parser.add_argument('--column', type=str, help='Nombre de la columna')

    # Scan de tablas segun rango de filas
    parser.add_argument('--start_row', type=str,
                        help='Clave de la fila de inicio (inclusive)')
    parser.add_argument('--stop_row', type=str,
                        help='Clave de la fila de fin (exclusiva)')

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
        output = tabla.get(args.row_key, args.column_family, args.column)
        print(output)

    elif args.comando == 'scan':
        if not args.tabla:
            parser.error(
                'Debe proporcionar el nombre de la tabla')

        # Asume que las familias de columnas se cargan desde el archivo JSON
        tabla = Table(args.tabla, [])
        tabla.load_from_json(args.tabla)
        output = tabla.scan(args.start_row, args.stop_row)
        print(output)

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
            row_count = file_manager.count(args.tabla)
            print(f"""\nNúmero de filas en la tabla '{
                  args.tabla}': {row_count}\n""")
        except ValueError as e:
            print(f"Error: {e}")

    elif args.comando == 'describe':
        if not args.tabla:
            parser.error(
                'Debe proporcionar el nombre de la tabla')
        file_manager.describe(args.tabla, args.tabla)

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
