# app/services/data_service.py

import math
import time

from flask import g
from sqlalchemy import distinct

from app.models.estate_models import EstateSell, EstateHouse
from ..models.estate_models import EstateDeal, EstateSell


# --- НОВЫЙ КЛАСС ДЛЯ ПАГИНАЦИИ ---
class ManualPagination:
    """Простой объект пагинации, который имитирует Flask-SQLAlchemy Pagination."""

    def __init__(self, page, per_page, total, items):
        self.page = page
        self.per_page = per_page
        self.total = total
        self.items = items

    @property
    def pages(self):
        if self.per_page == 0:
            return 0
        return math.ceil(self.total / self.per_page)

    @property
    def has_prev(self):
        return self.page > 1

    @property
    def has_next(self):
        return self.page < self.pages

    @property
    def prev_num(self):
        return self.page - 1 if self.has_prev else None

    @property
    def next_num(self):
        return self.page + 1 if self.has_next else None

    # Свойства first и last для совместимости с шаблоном
    @property
    def first(self):
        return (self.page - 1) * self.per_page + 1 if self.total > 0 else 0

    @property
    def last(self):
        return min(self.page * self.per_page, self.total)

    def iter_pages(self, left_edge=2, left_current=2, right_current=5, right_edge=2):
        last = 0
        for num in range(1, self.pages + 1):
            if num <= left_edge or \
                    (num > self.page - left_current - 1 and num < self.page + right_current) or \
                    num > self.pages - right_edge:
                if last + 1 != num:
                    yield None
                yield num
                last = num
def get_all_sell_statuses():
    """Возвращает список уникальных статусов ОБЪЕКТОВ (sells) из MySQL."""
    try:
        if not hasattr(g, 'mysql_db_session') or g.mysql_db_session is None:
            return []
        results = g.mysql_db_session.query(distinct(EstateSell.estate_sell_status_name)).filter(EstateSell.estate_sell_status_name.isnot(None)).all()
        statuses = [row[0] for row in results if row[0]]
        return sorted(statuses)
    except Exception as e:
        print(f"[DEBUG] ❌ КРИТИЧЕСКАЯ ОШИБКА в get_all_sell_statuses: {e}")
        return []
def get_all_deal_statuses():
    """Возвращает список уникальных статусов сделок из MySQL."""
    try:
        if not hasattr(g, 'mysql_db_session') or g.mysql_db_session is None:
            return []
        # Запрос на получение уникальных, непустых значений
        results = g.mysql_db_session.query(distinct(EstateDeal.deal_status_name)).filter(EstateDeal.deal_status_name.isnot(None)).all()
        statuses = [row[0] for row in results if row[0]]
        return sorted(statuses)
    except Exception as e:
        print(f"[DEBUG] ❌ КРИТИЧЕСКАЯ ОШИБКА в get_all_deal_statuses: {e}")
        return []
def get_sells_with_house_info(page, per_page):
    """
    Получает предложения о продаже для конкретной страницы из MySQL.
    Возвращает кастомный объект пагинации.
    """
    print(f"\n[DATA SERVICE] Запрос данных для страницы {page} ({per_page} записей на странице)...")
    start_time = time.time()
    try:
        if not hasattr(g, 'mysql_db_session') or g.mysql_db_session is None:
            print("[DATA SERVICE] ❌ ОШИБКА: Сессия mysql_db_session отсутствует в g.")
            return None

        # --- ИСПРАВЛЕННАЯ ЛОГИКА ПАГИНАЦИИ ---
        query = g.mysql_db_session.query(EstateSell).join(EstateHouse)
        total = query.order_by(None).count()
        items = query.order_by(EstateSell.id.desc()).limit(per_page).offset((page - 1) * per_page).all()

        # Используем наш новый класс
        pagination = ManualPagination(page, per_page, total, items)
        # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

        end_time = time.time()
        duration = round(end_time - start_time, 2)
        print(
            f"[DATA SERVICE] ✔️ Запрос для страницы {page} выполнен за {duration} сек. Найдено: {pagination.total} записей.")
        return pagination
    except Exception as e:
        import traceback
        print(f"[DATA SERVICE] ❌ ОШИБКА при запросе данных с пагинацией:")
        traceback.print_exc()
        return None


def get_all_complex_names():
    """Возвращает список уникальных названий ЖК из базы данных."""
    try:
        if not hasattr(g, 'mysql_db_session') or g.mysql_db_session is None:
            return []
        results = g.mysql_db_session.query(distinct(EstateHouse.complex_name)).all()
        complex_names = [row[0] for row in results]
        return complex_names
    except Exception as e:
        print(f"[DEBUG] ❌ КРИТИЧЕСКАЯ ОШИБКА в get_all_complex_names: {e}")
        return []


def get_filter_options():
    """
    Получает уникальные значения для фильтров этажей и комнат.
    """
    try:
        if not hasattr(g, 'mysql_db_session') or g.mysql_db_session is None:
            return {'floors': [], 'rooms': []}

        floors = sorted([f[0] for f in g.mysql_db_session.query(distinct(EstateSell.estate_floor)).filter(
            EstateSell.estate_floor.isnot(None)).all()])
        rooms = sorted([r[0] for r in g.mysql_db_session.query(distinct(EstateSell.estate_rooms)).filter(
            EstateSell.estate_rooms.isnot(None)).all()])

        return {'floors': floors, 'rooms': rooms}
    except Exception as e:
        print(f"[DATA SERVICE] ❌ ОШИБКА при запросе опций для фильтров: {e}")
        return {'floors': [], 'rooms': []}