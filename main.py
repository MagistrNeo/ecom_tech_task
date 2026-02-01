import pandas as pd
import argparse
import sys
import time
from data_generator import DataGenerator
from clickhouse_loader import ClickHouseLoader
from config import DATA_GENERATION_CONFIG, CLICKHOUSE_CONFIG
import traceback

def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description='Генерация и загрузка данных в ClickHouse')
    parser.add_argument('--rows', type=int, default=DATA_GENERATION_CONFIG['num_rows'],
                       help=f'Количество строк для генерации (по умолчанию: {DATA_GENERATION_CONFIG["num_rows"]:,})')
    parser.add_argument('--batch-size', type=int, default=DATA_GENERATION_CONFIG['batch_size'],
                       help=f'Размер батча для загрузки (по умолчанию: {DATA_GENERATION_CONFIG["batch_size"]:,})')
    parser.add_argument('--table', type=str, default='user_events',
                       help='Имя таблицы в ClickHouse (по умолчанию: user_events)')
    parser.add_argument('--database', type=str, default=CLICKHOUSE_CONFIG['database'],
                       help=f'Имя базы данных (по умолчанию: {CLICKHOUSE_CONFIG["database"]})')
    parser.add_argument('--generate-only', action='store_true',
                       help='Только генерация данных без загрузки в ClickHouse')

    args = parser.parse_args()
    
    print("=" * 80)
    print("CLICKHOUSE DATA LOADER")
    print("=" * 80)
    
    start_time = time.time()
    loader = ClickHouseLoader()
    try:
        # Режим генерации данных
        print(f"Генерация {args.rows:,} строк данных...")
        generator = DataGenerator()
        
        if args.rows <= 1000000:
            # Для меньших объемов генерируем сразу весь датафрейм
            df = generator.generate_dataframe(args.rows)
            
            if not args.generate_only:
                # Загрузка в ClickHouse
                print("\nПодключение к ClickHouse...")
                
                print(f"Создание таблицы {args.database}.{args.table}...")
                loader.create_table(args.table, args.database)
                
                print(f"Загрузка данных в ClickHouse...")
                loader.load_data(df, args.table, args.database, args.batch_size)
                
                loader.disconnect()
        
        else:
            # Для больших объемов используем генерацию батчами
            if not args.generate_only:
                print("\nПодключение к ClickHouse...")
                loader = ClickHouseLoader()
                
                print(f"Создание таблицы {args.database}.{args.table}...")
                loader.create_table(args.table, args.database)
            
            print(f"\nГенерация и загрузка данных батчами...")
            
            # Генерация данных батчами
            batch_generator = generator.generate_data_batches(args.batch_size)
            total_rows = 0
            
            for i, df_batch in enumerate(batch_generator):
                total_rows += len(df_batch)
                loader.load_data_batch(df_batch, args.table, args.database)
            
            print(f"\nЗагрузка завершена!")
            loader.verify_data_count(args.table, args.database)
            loader.disconnect()
        
        elapsed_time = time.time() - start_time
        print(f"\nОбщее время выполнения: {elapsed_time:.2f} секунд")
        print("=" * 80)
        
    except Exception as e:
        print(f"\nОшибка: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()