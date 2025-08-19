# app/services/settings_service.py

from app.core.extensions import db

# --- ИЗМЕНЕНИЯ ЗДЕСЬ: Обновляем импорты ---
from ..models import planning_models
from ..models.exclusion_models import ExcludedComplex
from flask import g, abort

def get_calculator_settings():
    """
    Получает настройки калькуляторов. Если их нет, создает по умолчанию.
    Использует паттерн "Синглтон", всегда работая с записью id=1.
    """
    # Используем planning_models.CalculatorSettings
    settings = g.company_db_session.query(planning_models.CalculatorSettings).get(1)
    if not settings:
        settings = planning_models.CalculatorSettings(id=1)
        g.company_db_session.add(settings)
        g.company_db_session.commit()
    return settings


def get_all_excluded_complexes():
    """Возвращает список всех исключенных ЖК."""
    return g.company_db_session.query(ExcludedComplex).order_by(ExcludedComplex.complex_name).all()


def toggle_complex_exclusion(complex_name: str):
    """
    Добавляет ЖК в список исключений, если его там нет,
    или удаляет, если он там уже есть.
    """
    try:
        existing = g.company_db_session.query(ExcludedComplex).filter_by(complex_name=complex_name).first()
        if existing:
            g.company_db_session.delete(existing)
            message = f"Проект '{complex_name}' был удален из списка исключений."
            category = "success"
        else:
            new_exclusion = ExcludedComplex(complex_name=complex_name)
            g.company_db_session.add(new_exclusion)
            message = f"Проект '{complex_name}' был добавлен в список исключений."
            category = "info"

        g.company_db_session.commit()
        print(f"[SETTINGS SERVICE] ✅ Успешно обновлен статус исключения для ЖК: {complex_name}")
    except Exception as e:
        g.company_db_session.rollback()
        print(f"[SETTINGS SERVICE] ❌ Ошибка при обновлении статуса исключения: {e}")
        message = "Произошла ошибка при обновлении статуса исключения."
        category = "danger"
    return message, category


def update_calculator_settings(form_data):
    """Обновляет настройки калькуляторов из данных формы."""
    settings = get_calculator_settings()

    settings.standard_installment_whitelist = form_data.get('standard_installment_whitelist', '')
    settings.dp_installment_whitelist = form_data.get('dp_installment_whitelist', '')
    settings.dp_installment_max_term = int(form_data.get('dp_installment_max_term', 6))
    settings.time_value_rate_annual = float(form_data.get('time_value_rate_annual', 16.5))
    settings.standard_installment_min_dp_percent = float(form_data.get('standard_installment_min_dp_percent', 15.0))

    g.company_db_session.commit()
