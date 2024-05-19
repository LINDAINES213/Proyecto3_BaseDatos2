import argparse
from HFileManager2 import HFileManager
from prettytable import PrettyTable

def print_table(data):
    """
    Imprime los datos en forma de tabla utilizando PrettyTable, mostrando solo el primer timestamp.
    """
    table = PrettyTable(['Columna', 'Valor'])
    for family, columns in data.items():
        for column, value in columns.items():
            first_timestamp = next(iter(value))
            first_timestamp_value = value[first_timestamp]
            table.add_row([f"{family}:{column}", first_timestamp_value])
    print(table)


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

    # Analizar los argumentos
    args = parser.parse_args()

    file_manager = HFileManager('./datos')

    # Ejecutar el comando correspondiente
    if args.comando == 'get':
        if not args.tabla or not args.archivo or not args.row_key:
            parser.error('Debe proporcionar el nombre de la tabla, la ruta del archivo JSON y la clave de la fila')

        table = file_manager.load_table(args.tabla)
        if table is None:
            print(f"La tabla '{args.tabla}' no existe.")
        else:
            output = table.get(args.row_key, args.column_family, args.column)
            print_table(output)

    elif args.comando == 'scan':
        if not args.tabla or not args.archivo:
            parser.error('Debe proporcionar el nombre de la tabla y la ruta del archivo JSON')

        table = file_manager.load_table(args.tabla)
        if table is None:
            print(f"La tabla '{args.tabla}' no existe.")
        else:
            output = table.scan(args.start_row, args.stop_row)
            print_table(output)

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
            print(f"\nNúmero de filas en la tabla '{args.tabla}': {row_count}\n")
        except ValueError as e:
            print(f"Error: {e}")

    else:
        parser.error(f'Comando "{args.comando}" no reconocido')



if __name__ == '__main__':
    main()