from clickhouse_driver import Client
import pandas as pd
from typing import Dict, Optional
from config import CLICKHOUSE_CONFIG
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickHouseLoader:
    
    def __init__(self, config: Optional[Dict] = None):

        if config is None:
            config = CLICKHOUSE_CONFIG
        
        self.config = config
        self.client = None
        self.connect()
    
    def connect(self):
        try:
            self.client = Client(**self.config)
            logger.info(f"Успешное подключение к ClickHouse: {self.config['host']}:{self.config['port']}")
            
            # Проверка соединения
            result = self.client.execute('SELECT version()')
            logger.info(f"Версия ClickHouse: {result[0][0]}")
            
        except Exception as e:
            logger.error(f"Ошибка подключения к ClickHouse: {e}")
            raise
    
    def disconnect(self):
        if self.client:
            self.client.disconnect()
            logger.info("Соединение с ClickHouse закрыто")
    
    def create_database(self, database_name: str):
        try:
            self.client.execute(f'CREATE DATABASE IF NOT EXISTS {database_name}')
            logger.info(f"База данных '{database_name}' создана или уже существует")
        except Exception as e:
            logger.error(f"Ошибка создания базы данных: {e}")
            raise
    
    def create_table(self, table_name: str = 'user_events', database_name: str = None):
        if database_name is None:
            database_name = self.config.get('database', 'default')
        
        # Создаем базу данных если нужно
        self.create_database(database_name)
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
            date Date,
            event String,
            device_id UUID,
            event_time DateTime,
            city String,
            device_os String
        )
        ENGINE = MergeTree()
        ORDER BY (date, event_time, device_id)
        PARTITION BY toYYYYMM(date)
        """
        
        try:
            self.client.execute(create_table_sql)
            logger.info(f"Таблица '{database_name}.{table_name}' создана или уже существует")
            
            # Показываем структуру таблицы
            self.show_table_structure(table_name, database_name)
            
        except Exception as e:
            logger.error(f"Ошибка создания таблицы: {e}")
            raise
    
    def show_table_structure(self, table_name: str, database_name: str = None):
        if database_name is None:
            database_name = self.config.get('database', 'default')
        
        try:
            structure = self.client.execute(
                f'DESCRIBE TABLE {database_name}.{table_name}'
            )
            
            logger.info(f"\nСтруктура таблицы '{database_name}.{table_name}':")
            logger.info("-" * 60)
            logger.info(f"{'Поле':<20} {'Тип':<20} {'Default':<10} {'Compression':<10}")
            logger.info("-" * 60)
            
            for row in structure:
                logger.info(f"{row[0]:<20} {row[1]:<20} {str(row[2]):<10} {str(row[3]):<10}")
            
        except Exception as e:
            logger.error(f"Ошибка получения структуры таблицы: {e}")
    
    def load_data(self, df: pd.DataFrame, table_name: str = 'user_events', 
                  database_name: str = None, batch_size: int = 50000):
        if database_name is None:
            database_name = self.config.get('database', 'default')
        
        if df.empty:
            logger.warning("DataFrame пустой, нечего загружать")
            return
        
        # Подготовка данных
        data = df.to_dict('records')
        
        # Определяем типы данных для ClickHouse
        column_types = {
            'date': 'Date',
            'event': 'String',
            'device_id': 'UUID',
            'event_time': 'DateTime',
            'city': 'String',
            'device_os': 'String'
        }
        
        # Загрузка данных батчами
        total_rows = len(data)
        num_batches = (total_rows + batch_size - 1) // batch_size
        
        logger.info(f"Начинаю загрузку {total_rows:,} строк в {num_batches} батчах...")
        
        for i in range(0, total_rows, batch_size):
            batch = data[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            try:
                # Конвертируем DataFrame в список кортежей для ClickHouse
                rows = []
                for row in batch:
                    rows.append((
                        row['date'],
                        row['event'],
                        row['device_id'],
                        row['event_time'],
                        row['city'],
                        row['device_os']
                    ))
                
                # Выполняем вставку
                insert_sql = f"""
                INSERT INTO {database_name}.{table_name} 
                (date, event, device_id, event_time, city, device_os) 
                VALUES
                """
                
                self.client.execute(insert_sql, rows)
                
                logger.info(f"Загружен батч {batch_num}/{num_batches} "
                          f"({len(batch):,} строк)")
                
            except Exception as e:
                logger.error(f"Ошибка загрузки батча {batch_num}: {e}")
                raise
        
        logger.info(f"Загрузка завершена! Всего загружено {total_rows:,} строк")
        
        # Проверяем количество загруженных строк
        self.verify_data_count(table_name, database_name)
    
    def load_data_batch(self, df_batch: pd.DataFrame, table_name: str = 'user_events',
                        database_name: str = None):
        if database_name is None:
            database_name = self.config.get('database', 'default')
        
        if df_batch.empty:
            return
        
        try:
            # Конвертируем DataFrame в список кортежей для ClickHouse
            rows = []
            for _, row in df_batch.iterrows():
                rows.append((
                    row['date'],
                    row['event'],
                    row['device_id'],
                    row['event_time'],
                    row['city'],
                    row['device_os']
                ))
            
            # Выполняем вставку
            insert_sql = f"""
            INSERT INTO {database_name}.{table_name} 
            (date, event, device_id, event_time, city, device_os) 
            VALUES
            """
            
            self.client.execute(insert_sql, rows)
            
            logger.info(f"Загружено {len(rows):,} строк в таблицу {database_name}.{table_name}")
            
        except Exception as e:
            logger.error(f"Ошибка загрузки данных: {e}")
            raise
    
    def verify_data_count(self, table_name: str = 'user_events', database_name: str = None):

        if database_name is None:
            database_name = self.config.get('database', 'default')
        
        try:
            result = self.client.execute(
                f'SELECT COUNT(*) FROM {database_name}.{table_name}'
            )
            count = result[0][0]
            logger.info(f"Количество строк в таблице '{database_name}.{table_name}': {count:,}")
            
            # Получаем статистику по событиям
            event_stats = self.client.execute(
                f'''SELECT event, COUNT(*) as count 
                FROM {database_name}.{table_name} 
                GROUP BY event 
                ORDER BY count DESC'''
            )
            
            logger.info("\nРаспределение событий в ClickHouse:")
            for event, count in event_stats:
                logger.info(f"  {event}: {count:,}")
                
        except Exception as e:
            logger.error(f"Ошибка проверки данных: {e}")
    
    def execute_query(self, query: str):
        try:
            result = self.client.execute(query)
            return result
        except Exception as e:
            logger.error(f"Ошибка выполнения запроса: {e}")
            raise


if __name__ == "__main__":
    # Пример использования
    loader = ClickHouseLoader()
    
    # Создание таблицы
    loader.create_table('test_events', 'default')
    
    # Пример создания тестовых данных
    test_data = pd.DataFrame([{
        'date': '2025-01-01',
        'event': 'view',
        'device_id': '123e4567-e89b-12d3-a456-426614174000',
        'event_time': '2025-01-01 12:12:21',
        'city': 'Москва',
        'device_os': 'Android'
    }])
    
    # Загрузка тестовых данных
    loader.load_data(test_data, 'test_events')
    
    # Проверка данных
    loader.verify_data_count('test_events')
    
    # Закрытие соединения
    loader.disconnect()