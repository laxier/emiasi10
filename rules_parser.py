"""
Парсер правил отслеживания расписания врачей.
Независимый модуль без зависимостей от aiogram - для использования в веб-приложении.
"""
import re
from datetime import datetime, timedelta


def normalize_time_range(tr):
    if not tr:
        return tr
    tr = tr.strip()
    # Замена тире различных типов на стандартное время
    tr = re.sub(r'[-–—]', '-', tr)
    # Убираем пробелы вокруг тире
    tr = re.sub(r'\s*-\s*', '-', tr)
    # Проверяем на формат HH:MM-HH:MM или H:MM-HH:MM
    match = re.match(r'^(\d{1,2}):?(\d{2})\s*-\s*(\d{1,2}):?(\d{2})$', tr.replace(':', ''))
    if match:
        start_h, start_m, end_h, end_m = match.groups()
        start = f"{int(start_h):02d}:{start_m}"
        end = f"{int(end_h):02d}:{end_m}"
        return f"{start}-{end}"
    return tr


def _parse_date_rule(date_str, default_year=None):
    """
    Парсит дату из строки в различных форматах:
    - "25 марта", "10 октября" - с русскими названиями месяцев
    - "2024-03-25" - ISO формат
    - "25.03", "10.10" - DD.MM формат
    """
    if not date_str:
        return None
    
    date_str = date_str.strip().lower()
    
    # ISO формат YYYY-MM-DD
    iso_match = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', date_str)
    if iso_match:
        year, month, day = map(int, iso_match.groups())
        try:
            return datetime(year, month, day).date()
        except ValueError:
            return None
    
    # DD.MM формат
    dot_match = re.match(r'^(\d{1,2})\.(\d{1,2})$', date_str)
    if dot_match:
        day, month = map(int, dot_match.groups())
        year = default_year or datetime.now().year
        try:
            return datetime(year, month, day).date()
        except ValueError:
            return None
    
    # Русские месяцы
    russian_months = {
        'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
        'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
        'янв': 1, 'фев': 2, 'мар': 3, 'апр': 4, 'май': 5, 'июн': 6,
        'июл': 7, 'авг': 8, 'сен': 9, 'окт': 10, 'ноя': 11, 'дек': 12
    }
    
    # Попробуем формат "DD месяц"
    parts = date_str.split()
    if len(parts) == 2:
        try:
            day = int(parts[0])
            month_name = parts[1]
            if month_name in russian_months:
                month = russian_months[month_name]
                year = default_year or datetime.now().year
                return datetime(year, month, day).date()
        except (ValueError, IndexError):
            pass
    
    return None


def parse_user_tracking_input(text: str):
    """
    Условная функция, разбивающая строку вида:
    "понедельник: 08:00-12:00, 25 марта: 09:00-11:00"
    или с другими разделителями: "10 октября — 15 — 00-17:00"
    на массив правил:
    [
      {
        "type": "weekday",
        "value": "понедельник",
        "timeRanges": ["08:00-12:00"]
      },
      {
        "type": "date",
        "value": "25 марта",
        "timeRanges": ["09:00-11:00"]
      }
    ]
    """
    if not text:
        return []

    separators = [":", " "]  # Двоеточие и пробел как разделители

    parts = [p.strip() for p in text.split(",")]
    rules = []
    for part in parts:
        # Специальная обработка для паттернов с пробелом
        part_lower = part.lower().strip()
        
        # Проверяем паттерн "сегодня/завтра/день_недели HH:MM-HH:MM" 
        weekdays = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
        relative_days = ["сегодня", "завтра"]

        # 1. ISO дата + интервал: 2025-10-10 16:00-17:30
        m_iso = re.match(r'^(\d{4}-\d{2}-\d{2})\s+(\d{1,2}:\d{2}\s*[-–—]\s*\d{1,2}:\d{2})$', part_lower)
        if m_iso:
            date_val, tr = m_iso.groups()
            tr_norm = normalize_time_range(tr)
            rules.append({
                "type": "date",
                "value": date_val,  # будет позже нормализовано в ISO ещё раз (idempotent)
                "timeRanges": [tr_norm]
            })
            continue

        # 2. DD.MM(.YYYY) + интервал: 10.10 08:00-12:00 или 10.10.2025 08:00-12:00
        m_dm = re.match(r'^(\d{1,2}\.\d{1,2}(?:\.\d{4})?)\s+(\d{1,2}:\d{2}\s*[-–—]\s*\d{1,2}:\d{2})$', part_lower)
        if m_dm:
            date_val, tr = m_dm.groups()
            tr_norm = normalize_time_range(tr)
            rules.append({
                "type": "date",
                "value": date_val,
                "timeRanges": [tr_norm]
            })
            continue

        # 3. Русское название даты "25 марта 09:00-11:00"
        m_rus = re.match(r'^(\d{1,2}\s+[а-яё]{3,}?)\s+(\d{1,2}:\d{2}\s*[-–—]\s*\d{1,2}:\d{2})$', part_lower)
        if m_rus:
            date_val, tr = m_rus.groups()
            tr_norm = normalize_time_range(tr)
            rules.append({
                "type": "date",
                "value": date_val,
                "timeRanges": [tr_norm]
            })
            continue
        
        # Ищем паттерн: (день) (время)
        space_idx = part_lower.find(' ')
        if space_idx != -1 and re.search(r'\d{1,2}:\d{2}-\d{1,2}:\d{2}', part):
            day_word = part_lower[:space_idx].strip()
            time_part = part[space_idx+1:].strip()
            
            if day_word in relative_days or day_word in weekdays:
                rule_type = "date" if day_word in relative_days else "weekday"
                rules.append({
                    "type": rule_type,
                    "value": day_word,
                    "timeRanges": [time_part]
                })
                continue
        # Найти первый двоеточие-разделитель "день: интервалы" если ещё не обработали кейсы
        sep_index = part.find(':')
        if sep_index == -1:
            # Нет двоеточия => трактуем как просто день/дата без интервалов
            day_val = part_lower
            rules.append({
                "type": "weekday" if day_val in weekdays else "date",
                "value": day_val,
                "timeRanges": []
            })
            continue

        day_part = part[:sep_index].strip()
        time_part = part[sep_index + 1:].strip()
        time_part = re.sub(r'[—–]', '-', time_part)
        day_val = day_part.lower()
        timeRanges = [t.strip() for t in time_part.split(';') if t.strip()]
        rule_type = "weekday" if day_val in weekdays else "date"
        rules.append({
            "type": rule_type,
            "value": day_val,
            "timeRanges": timeRanges
        })

    # Нормализуем правила + ФИКСАЦИЯ относительных значений в момент ввода
    today = datetime.now().date()
    current_wd = datetime.now().weekday()
    for rule in rules:
        rule['value'] = rule['value'].strip().lower()
        rule['timeRanges'] = [normalize_time_range(tr) for tr in rule['timeRanges']]
        # Заморозка только относительных ('сегодня','завтра'), weekday оставляем как повторяющиеся
        if rule['type'] == 'date' and rule['value'] in ('сегодня', 'завтра'):
            target = today if rule['value'] == 'сегодня' else today + timedelta(days=1)
            rule['value'] = target.strftime('%Y-%m-%d')
        elif rule['type'] == 'weekday':
            # ничего не делаем – правило остаётся повторяющимся
            pass
        if rule['type'] == 'date':
            parsed = _parse_date_rule(rule['value'], datetime.now().year)
            if parsed:
                # Приводим к ISO
                rule['value'] = parsed.strftime('%Y-%m-%d')

    return rules