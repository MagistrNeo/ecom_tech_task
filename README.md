# ClickHouse Data Loader

Этот проект генерирует синтетические данные о событиях пользователей и загружает их в ClickHouse.

## Функциональность

1. Генерация датафрейма с 5+ миллионами строк
2. Создание таблицы в ClickHouse
3. Загрузка данных в ClickHouse

## Структура данных

- `date` - дата события (2025-01-01 - 2025-12-31)
- `event` - название события (view, purchase, add_to_cart)
- `device_id` - GUID устройства
- `event_time` - дата и время события
- `city` - город клиента
- `device_os` - ОС устройства (Android, iOS)

## Требования

- Python 3.8+
- ClickHouse сервер
- Установленные библиотеки из requirements.txt

## Установка

```bash
pip install -r requirements.txt
