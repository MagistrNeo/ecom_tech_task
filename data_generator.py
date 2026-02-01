import pandas as pd
import numpy as np
import uuid
from datetime import datetime, timedelta
import random
from typing import Dict, List
from tqdm import tqdm
from config import DATA_GENERATION_CONFIG, CITIES, EVENTS, DEVICE_OS


class DataGenerator:
    
    def __init__(self):
        self.config = DATA_GENERATION_CONFIG
        self.cities = CITIES
        self.events = EVENTS
        self.device_os = DEVICE_OS
        
    def generate_dataframe(self, num_rows: int = None) -> pd.DataFrame:
        
        if num_rows is None:
            num_rows = self.config['num_rows']
        
        print(f"Генерация {num_rows:,} строк данных...")
        
        # Определяем количество уникальных устройств (10% от общего числа строк)
        unique_devices = max(1000, int(num_rows * 0.1))
        
        # Генерируем уникальные device_id
        print("Генерация уникальных device_id...")
        device_ids = [str(uuid.uuid4()) for _ in range(unique_devices)]
        
        # Создаем словарь с характеристиками для каждого устройства
        print("Назначение характеристик устройствам...")
        device_profiles = {}
        for device_id in device_ids:
            device_profiles[device_id] = {
                'city': random.choice(self.cities),
                'device_os': random.choice(self.device_os),
                'created_date': self._random_date(
                    self.config['start_date'], 
                    self.config['end_date']
                )
            }
        
        data = []
        # Используем tqdm для отображения прогресса
        for i in tqdm(range(num_rows), desc="Генерация строк"):
            # Выбираем случайное устройство
            device_id = random.choice(device_ids)
            profile = device_profiles[device_id]
            
            # Определяем дату события (не раньше даты создания устройства)
            event_date = self._random_date(
                profile['created_date'].strftime('%Y-%m-%d'),
                self.config['end_date']
            )
            
            # Генерируем время события
            event_time = self._random_datetime(event_date)
            
            # Выбираем событие с разной вероятностью
            event = self._weighted_random_event()
            
            data.append({
                'date': event_date,
                'event': event,
                'device_id': device_id,
                'event_time': event_time,
                'city': profile['city'],
                'device_os': profile['device_os']
            })
        
        # Создаем датафрейм
        df = pd.DataFrame(data)
        
        # Сортируем по времени события
        df = df.sort_values('event_time').reset_index(drop=True)
        
        print(f"Сгенерировано {len(df):,} строк")
        print(f"Уникальных устройств: {len(device_ids):,}")
        print(f"Диапазон дат: от {df['date'].min()} до {df['date'].max()}")
        print(f"\nРаспределение событий:")
        print(df['event'].value_counts())
        print(f"\nРаспределение по городам (ТОП-5):")
        print(df['city'].value_counts().head())
        print(f"\nРаспределение по ОС:")
        print(df['device_os'].value_counts())
        
        return df
    
    def generate_data_batches(self, batch_size: int = None):
    
        if batch_size is None:
            batch_size = self.config['batch_size']
        
        total_rows = self.config['num_rows']
        num_batches = (total_rows + batch_size - 1) // batch_size
        
        print(f"Генерация {total_rows:,} строк в {num_batches} батчах...")
        
        # Определяем количество уникальных устройств
        unique_devices = max(1000, int(total_rows * 0.1))
        
        # Генерируем уникальные device_id
        device_ids = [str(uuid.uuid4()) for _ in range(unique_devices)]
        
        # Создаем словарь с характеристиками для каждого устройства
        device_profiles = {}
        for device_id in device_ids:
            device_profiles[device_id] = {
                'city': random.choice(self.cities),
                'device_os': random.choice(self.device_os),
                'created_date': self._random_date(
                    self.config['start_date'], 
                    self.config['end_date']
                )
            }
        
        for batch_num in range(num_batches):
            batch_data = []
            current_batch_size = min(batch_size, total_rows - batch_num * batch_size)
            
            for _ in range(current_batch_size):
                # Выбираем случайное устройство
                device_id = random.choice(device_ids)
                profile = device_profiles[device_id]
                
                # Определяем дату события
                event_date = self._random_date(
                    profile['created_date'].strftime('%Y-%m-%d'),
                    self.config['end_date']
                )
                
                # Генерируем время события
                event_time = self._random_datetime(event_date)
                
                # Выбираем событие с разной вероятностью
                event = self._weighted_random_event()
                
                batch_data.append({
                    'date': event_date,
                    'event': event,
                    'device_id': device_id,
                    'event_time': event_time,
                    'city': profile['city'],
                    'device_os': profile['device_os']
                })
            
            df_batch = pd.DataFrame(batch_data)
            yield df_batch
            
            print(f"Сгенерирован батч {batch_num + 1}/{num_batches} "
                  f"({len(df_batch):,} строк)")
    
    def _random_date(self, start_date: str, end_date: str) -> datetime.date:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        delta = end - start
        random_days = random.randint(0, delta.days)
        
        return (start + timedelta(days=random_days)).date()
    
    def _random_datetime(self, date: datetime.date) -> datetime:
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        return datetime.combine(date, datetime.min.time()).replace(
            hour=hour, minute=minute, second=second
        )
    
    def _weighted_random_event(self) -> str:
        weights = [0.7, 0.2, 0.1]  # view: 70%, purchase: 20%, add_to_cart: 10%
        return random.choices(self.events, weights=weights, k=1)[0]


if __name__ == "__main__":
    # Пример использования
    generator = DataGenerator()
    
    # Генерация небольшого датафрейма для тестирования
    test_df = generator.generate_dataframe(1000)
    print("\nПервые 5 строк сгенерированных данных:")
    print(test_df.head())
    print(f"\nИнформация о датафрейме:")
    print(test_df.info())