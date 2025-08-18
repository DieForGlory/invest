# app/services/settings_service.py

from app.core.extensions import db

# --- ИЗМЕНЕНИЯ ЗДЕСЬ: Обновляем импорты ---
from ..models import planning_models
from ..models.exclusion_models import ExcludedComplex


def get_calculator_settings():
    """
    Получает настройки калькуляторов. Если их нет, создает по умолчанию.
    Использует паттерн "Синглтон", всегда работая с записью id=1.
    """
    # Используем planning_models.CalculatorSettings
    settings = planning_models.CalculatorSettings.query.get(1)
    if not settings:
        settings = planning_models.CalculatorSettings(id=1)
        db.session.add(settings)
        db.session.commit()
    return settings


def get_all_excluded_complexes():
    """Возвращает список всех исключенных ЖК."""
    return ExcludedComplex.query.order_by(ExcludedComplex.complex_name).all()


def toggle_complex_exclusion(complex_name: str):
    """
    Добавляет ЖК в список исключений, если его там нет,
    или удаляет, если он там уже есть.
    """
    existing = ExcludedComplex.query.filter_by(complex_name=complex_name).first()
    if existing:
        db.session.delete(existing)
        message = f"Проект '{complex_name}' был удален из списка исключений."
        category = "success"
    else:
        new_exclusion = ExcludedComplex(complex_name=complex_name)
        db.session.add(new_exclusion)
        message = f"Проект '{complex_name}' был добавлен в список исключений."
        category = "info"

    db.session.commit()
    return message, category


def update_calculator_settings(form_data):
    """Обновляет настройки калькуляторов из данных формы."""
    settings = get_calculator_settings()

    settings.standard_installment_whitelist = form_data.get('standard_installment_whitelist', '')
    settings.dp_installment_whitelist = form_data.get('dp_installment_whitelist', '')
    settings.dp_installment_max_term = int(form_data.get('dp_installment_max_term', 6))
    settings.time_value_rate_annual = float(form_data.get('time_value_rate_annual', 16.5))
    settings.standard_installment_min_dp_percent = float(form_data.get('standard_installment_min_dp_percent', 15.0))

    db.session.commit()