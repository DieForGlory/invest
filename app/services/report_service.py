# app/services/report_service.py
import pandas as pd
import numpy as np
from datetime import date, timedelta
from sqlalchemy import func, extract, case
from flask import g
import io
from collections import defaultdict
from flask_login import current_user
from app.models import planning_models
from .data_service import get_all_complex_names
from ..models.estate_models import EstateDeal, EstateHouse, EstateSell
from ..models.finance_models import FinanceOperation
import json

def generate_consolidated_report_by_period(year: int, period: str, property_type: str):
    """
    Генерирует сводный отчет за период (квартал, полугодие), суммируя данные по месяцам.
    """
    PERIOD_MONTHS = {
        'q1': range(1, 4), 'q2': range(4, 7), 'q3': range(7, 10),
        'q4': range(10, 13), 'h1': range(1, 7), 'h2': range(7, 13),
    }
    months_in_period = PERIOD_MONTHS.get(period)
    if not months_in_period:
        return [], {}

    aggregated_data = defaultdict(lambda: defaultdict(float))
    aggregated_totals = defaultdict(float)

    for month in months_in_period:
        monthly_data, monthly_totals, _ = generate_plan_fact_report(year, month, property_type)
        for project_row in monthly_data:
            complex_name = project_row['complex_name']
            for key, value in project_row.items():
                if key != 'complex_name' and isinstance(value, (int, float)):
                    aggregated_data[complex_name][key] += value
            aggregated_data[complex_name]['complex_name'] = complex_name
        for key, value in monthly_totals.items():
            if isinstance(value, (int, float)):
                aggregated_totals[key] += value

    final_report_data = []
    for complex_name, data in aggregated_data.items():
        data['percent_fact_units'] = (data['fact_units'] / data['plan_units'] * 100) if data.get('plan_units',
                                                                                                 0) > 0 else 0
        data['percent_fact_volume'] = (data['fact_volume'] / data['plan_volume'] * 100) if data.get('plan_volume',
                                                                                                    0) > 0 else 0
        data['percent_fact_income'] = (data['fact_income'] / data['plan_income'] * 100) if data.get('plan_income',
                                                                                                    0) > 0 else 0
        data['forecast_units'] = 0
        data['forecast_volume'] = 0
        final_report_data.append(dict(data))

    if aggregated_totals.get('plan_units', 0) > 0:
        aggregated_totals['percent_fact_units'] = (
                    aggregated_totals['fact_units'] / aggregated_totals['plan_units'] * 100)
    if aggregated_totals.get('plan_volume', 0) > 0:
        aggregated_totals['percent_fact_volume'] = (
                    aggregated_totals['fact_volume'] / aggregated_totals['plan_volume'] * 100)
    if aggregated_totals.get('plan_income', 0) > 0:
        aggregated_totals['percent_fact_income'] = (
                    aggregated_totals['fact_income'] / aggregated_totals['plan_income'] * 100)

    aggregated_totals['forecast_units'] = 0
    aggregated_totals['forecast_volume'] = 0
    final_report_data.sort(key=lambda x: x['complex_name'])
    return final_report_data, dict(aggregated_totals)


def get_fact_income_data(year: int, month: int, property_type: str):
    """Собирает ФАКТИЧЕСКИЕ поступления (статус 'Проведено') из MySQL."""
    results = g.mysql_db_session.query(
        EstateHouse.complex_name, func.sum(FinanceOperation.summa).label('fact_income')
    ).join(EstateSell, FinanceOperation.estate_sell_id == EstateSell.id) \
        .join(EstateHouse, EstateSell.house_id == EstateHouse.id) \
        .filter(
        FinanceOperation.status_name == "Проведено",
        extract('year', FinanceOperation.date_added) == year,
        extract('month', FinanceOperation.date_added) == month,
        FinanceOperation.payment_type != "Возврат поступлений при отмене сделки",
        FinanceOperation.payment_type != "Уступка права требования",
        EstateSell.estate_sell_category == property_type
    ).group_by(EstateHouse.complex_name).all()
    return {row.complex_name: (row.fact_income or 0) for row in results}


def get_expected_income_data(year: int, month: int, property_type: str):
    """Собирает ОЖИДАЕМЫЕ поступления из MySQL."""
    results = g.mysql_db_session.query(
        EstateHouse.complex_name,
        func.sum(FinanceOperation.summa).label('expected_income'),
        func.group_concat(FinanceOperation.id).label('income_ids')
    ).join(EstateSell, FinanceOperation.estate_sell_id == EstateSell.id) \
        .join(EstateHouse, EstateSell.house_id == EstateHouse.id) \
        .filter(
        FinanceOperation.status_name == "К оплате",
        extract('year', FinanceOperation.date_to) == year,
        extract('month', FinanceOperation.date_to) == month,
        EstateSell.estate_sell_category == property_type,
        FinanceOperation.payment_type != "Возврат поступлений при отмене сделки"
    ).group_by(EstateHouse.complex_name).all()

    data = {}
    for row in results:
        ids = [int(id_str) for id_str in row.income_ids.split(',')] if row.income_ids else []
        data[row.complex_name] = {'sum': row.expected_income or 0, 'ids': ids}
    return data


def get_refund_data(year: int, month: int, property_type: str):
    """Собирает данные по ВОЗВРАТАМ из MySQL."""
    results = g.mysql_db_session.query(
        func.sum(FinanceOperation.summa).label('total_refunds')
    ).join(EstateSell, FinanceOperation.estate_sell_id == EstateSell.id) \
        .filter(
        FinanceOperation.status_name == "К оплате",
        extract('year', FinanceOperation.date_to) == year,
        extract('month', FinanceOperation.date_to) == month,
        EstateSell.estate_sell_category == property_type,
        FinanceOperation.payment_type == "Возврат поступлений при отмене сделки"
    ).scalar()

    return results or 0.0


def get_plan_income_data(year: int, month: int, property_type: str):
    """Получает плановые данные по поступлениям из локальной SQLite."""
    results = g.company_db_session.query(planning_models.SalesPlan).filter_by(year=year, month=month,
                                                                              property_type=property_type).all()
    return {row.complex_name: row.plan_income for row in results}


def generate_ids_excel(ids_str: str):
    """Создает Excel-файл из списка ID."""
    try:
        ids = [int(id_val) for id_val in ids_str.split(',')]
    except (ValueError, AttributeError):
        return None

    df = pd.DataFrame(ids, columns=['ID Финансовой операции'])
    output = io.BytesIO()
    df.to_excel(output, index=False, sheet_name='IDs')
    output.seek(0)
    return output


def get_fact_data(year: int, month: int, property_type: str):
    """Собирает фактические данные о продажах из MySQL."""
    effective_date = func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date)
    sold_statuses = current_user.company.sale_statuses
    query = g.mysql_db_session.query(
        EstateHouse.complex_name,
        func.count(EstateDeal.id).label('fact_units')
    ).join(
        EstateSell, EstateDeal.estate_sell_id == EstateSell.id
    ).join(
        EstateHouse, EstateSell.house_id == EstateHouse.id
    ).filter(
        EstateDeal.deal_status_name.in_(sold_statuses),
        effective_date.isnot(None),
        extract('year', effective_date) == year,
        extract('month', effective_date) == month,
        EstateSell.estate_sell_category == property_type
    ).group_by(EstateHouse.complex_name)
    results = query.all()
    return {row.complex_name: row.fact_units for row in results}


def get_plan_data(year: int, month: int, property_type: str):
    """Получает плановые данные из локальной SQLite."""
    results = g.company_db_session.query(planning_models.SalesPlan).filter_by(
        year=year, month=month, property_type=property_type
    ).all()
    return {row.complex_name: row.plan_units for row in results}


def generate_plan_fact_report(year: int, month: int, property_type: str):
    """Основная функция для генерации отчета..."""
    # --- НАЧАЛО ИСПРАВЛЕНИЯ ---
    # Словарь для "перевода" названий с фронтенда в системные имена для БД MySQL
    prop_type_map = {
        'Квартира': 'flat',
        'Коммерческое помещение': 'comm',
        'Парковка': 'garage',
        'Кладовое помещение': 'storageroom'
    }
    # Получаем системное имя. Если его нет, используем исходное значение.
    property_type_for_fact_db = prop_type_map.get(property_type, property_type)

    # Для ПЛАНОВ используем русское название (property_type)
    plan_units_data = get_plan_data(year, month, property_type)
    plan_volume_data = get_plan_volume_data(year, month, property_type)
    plan_income_data = get_plan_income_data(year, month, property_type)

    # Для ФАКТОВ используем "переведенное" системное имя (property_type_for_fact_db)
    fact_units_data = get_fact_data(year, month, property_type_for_fact_db)
    fact_volume_data = get_fact_volume_data(year, month, property_type_for_fact_db)
    fact_income_data = get_fact_income_data(year, month, property_type_for_fact_db)
    all_expected_income_data = get_expected_income_data(year, month, property_type_for_fact_db)
    total_refunds = get_refund_data(year, month, property_type_for_fact_db)

    # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

    all_complexes = sorted(
        list(set(plan_units_data.keys()) | set(fact_units_data.keys()) | set(plan_income_data.keys())))

    report_data = []
    totals = {
        'plan_units': 0, 'fact_units': 0, 'plan_volume': 0, 'fact_volume': 0,
        'plan_income': 0, 'fact_income': 0, 'expected_income': 0
    }

    today = date.today()
    workdays_in_month = np.busday_count(f'{year}-{month:02d}-01',
                                        f'{year}-{month + 1:02d}-01' if month < 12 else f'{year + 1}-01-01')
    passed_workdays = np.busday_count(f'{year}-{month:02d}-01',
                                      today) if today.month == month and today.year == year else workdays_in_month
    passed_workdays = max(1, passed_workdays)

    for complex_name in all_complexes:
        plan_units = plan_units_data.get(complex_name, 0)
        fact_units = fact_units_data.get(complex_name, 0)
        plan_volume = plan_volume_data.get(complex_name, 0)
        fact_volume = fact_volume_data.get(complex_name, 0)
        plan_income = plan_income_data.get(complex_name, 0)
        fact_income = fact_income_data.get(complex_name, 0)
        complex_expected_income = all_expected_income_data.get(complex_name, {'sum': 0, 'ids': []})

        percent_fact_units = (fact_units / plan_units) * 100 if plan_units > 0 else 0
        forecast_units = ((
                                  fact_units / passed_workdays) * workdays_in_month / plan_units) * 100 if plan_units > 0 else 0
        percent_fact_volume = (fact_volume / plan_volume) * 100 if plan_volume > 0 else 0
        forecast_volume = ((
                                   fact_volume / passed_workdays) * workdays_in_month / plan_volume) * 100 if plan_volume > 0 else 0
        percent_fact_income = (fact_income / plan_income) * 100 if plan_income > 0 else 0

        totals['plan_units'] += plan_units
        totals['fact_units'] += fact_units
        totals['plan_volume'] += plan_volume
        totals['fact_volume'] += fact_volume
        totals['plan_income'] += plan_income
        totals['fact_income'] += fact_income
        totals['expected_income'] += complex_expected_income['sum']
        totals.setdefault('expected_income_ids', []).extend(complex_expected_income['ids'])

        report_data.append({
            'complex_name': complex_name,
            'plan_units': plan_units, 'fact_units': fact_units, 'percent_fact_units': percent_fact_units,
            'forecast_units': forecast_units,
            'plan_volume': plan_volume, 'fact_volume': fact_volume, 'percent_fact_volume': percent_fact_volume,
            'forecast_volume': forecast_volume,
            'plan_income': plan_income, 'fact_income': fact_income, 'percent_fact_income': percent_fact_income,
            'expected_income': complex_expected_income
        })

    totals['percent_fact_units'] = (totals['fact_units'] / totals['plan_units']) * 100 if totals[
                                                                                              'plan_units'] > 0 else 0
    totals['forecast_units'] = ((totals['fact_units'] / passed_workdays) * workdays_in_month / totals[
        'plan_units']) * 100 if totals['plan_units'] > 0 else 0
    totals['percent_fact_volume'] = (totals['fact_volume'] / totals['plan_volume']) * 100 if totals[
                                                                                                 'plan_volume'] > 0 else 0
    totals['forecast_volume'] = ((totals['fact_volume'] / passed_workdays) * workdays_in_month / totals[
        'plan_volume']) * 100 if totals['plan_volume'] > 0 else 0
    totals['percent_fact_income'] = (totals['fact_income'] / totals['plan_income']) * 100 if totals[
                                                                                                 'plan_income'] > 0 else 0

    return report_data, totals, total_refunds


def process_plan_from_excel(file_path: str, year: int, month: int):
    """Записывает планы в локальную SQLite."""
    df = pd.read_excel(file_path)
    for index, row in df.iterrows():
        plan_entry = g.company_db_session.query(planning_models.SalesPlan).filter_by(
            year=year, month=month, complex_name=row['ЖК'], property_type=row['Тип недвижимости']
        ).first()
        if not plan_entry:
            plan_entry = planning_models.SalesPlan(year=year, month=month, complex_name=row['ЖК'],
                                                   property_type=row['Тип недвижимости'])
            g.company_db_session.add(plan_entry)
        plan_entry.plan_units = row['План, шт']
        plan_entry.plan_volume = row['План контрактации, UZS']
        plan_entry.plan_income = row['План поступлений, UZS']
    g.company_db_session.commit()
    return f"Успешно обработано {len(df)} строк."


def generate_plan_template_excel():
    """Генерирует шаблон, используя названия ЖК из MySQL."""
    complex_names = get_all_complex_names()
    property_types = list(planning_models.PropertyType)
    headers = ['ЖК', 'Тип недвижимости', 'План, шт', 'План контрактации, UZS', 'План поступлений, UZS']
    data = [{'ЖК': name, 'Тип недвижимости': prop_type.value, 'План, шт': 0, 'План контрактации, UZS': 0,
             'План поступлений, UZS': 0} for name in complex_names for prop_type in property_types]
    df = pd.DataFrame(data, columns=headers)
    output = io.BytesIO()
    df.to_excel(output, index=False, sheet_name='Шаблон плана')
    output.seek(0)
    return output


def get_monthly_summary_by_property_type(year: int, month: int):
    """Собирает сводку, комбинируя данные из SQLite (планы) и MySQL (факты)."""
    summary_data = []
    property_types = list(planning_models.PropertyType)
    today = date.today()
    workdays_in_month = np.busday_count(f'{year}-{month:02d}-01',
                                        f'{year}-{month + 1:02d}-01' if month < 12 else f'{year + 1}-01-01')
    passed_workdays = np.busday_count(f'{year}-{month:02d}-01', today.strftime(
        '%Y-%m-%d')) if today.month == month and today.year == year else workdays_in_month
    passed_workdays = max(1, passed_workdays)

    for prop_type in property_types:
        total_plan_units = sum(get_plan_data(year, month, prop_type.value).values())
        total_fact_units = sum(get_fact_data(year, month, prop_type.name).values())  # Используем .name для 'FLAT'
        total_plan_volume = sum(get_plan_volume_data(year, month, prop_type.value).values())
        total_fact_volume = sum(
            get_fact_volume_data(year, month, prop_type.name).values())  # Используем .name для 'FLAT'
        total_plan_income = sum(get_plan_income_data(year, month, prop_type.value).values())
        total_fact_income = sum(
            get_fact_income_data(year, month, prop_type.name).values())  # Используем .name для 'FLAT'
        expected_income_data = get_expected_income_data(year, month, prop_type.name)  # Используем .name для 'FLAT'
        total_expected_income_sum = sum(v['sum'] for v in expected_income_data.values())
        total_expected_income_ids = [id_val for v in expected_income_data.values() for id_val in v['ids']]

        if (
                total_plan_units + total_fact_units + total_plan_volume + total_fact_volume + total_plan_income + total_fact_income) == 0:
            continue

        percent_fact_units = (total_fact_units / total_plan_units) * 100 if total_plan_units > 0 else 0
        forecast_units = ((
                                      total_fact_units / passed_workdays) * workdays_in_month / total_plan_units) * 100 if total_plan_units > 0 else 0
        percent_fact_volume = (total_fact_volume / total_plan_volume) * 100 if total_plan_volume > 0 else 0
        forecast_volume = ((
                                       total_fact_volume / passed_workdays) * workdays_in_month / total_plan_volume) * 100 if total_plan_volume > 0 else 0
        percent_fact_income = (total_fact_income / total_plan_income) * 100 if total_plan_income > 0 else 0

        summary_data.append({
            'property_type': prop_type.value,
            'total_plan_units': total_plan_units,
            'total_fact_units': total_fact_units,
            'percent_fact_units': percent_fact_units,
            'forecast_units': forecast_units,
            'total_plan_volume': total_plan_volume,
            'total_fact_volume': total_fact_volume,
            'percent_fact_volume': percent_fact_volume,
            'forecast_volume': forecast_volume,
            'total_plan_income': total_plan_income,
            'total_fact_income': total_fact_income,
            'percent_fact_income': percent_fact_income,
            'total_expected_income': {
                'sum': total_expected_income_sum,
                'ids': total_expected_income_ids
            }
        })
    return summary_data


def get_fact_volume_data(year: int, month: int, property_type: str):
    """Собирает фактические данные об объеме контрактации из MySQL."""
    effective_date = func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date)
    sold_statuses = current_user.company.sale_statuses
    results = g.mysql_db_session.query(
        EstateHouse.complex_name, func.sum(EstateDeal.deal_sum).label('fact_volume')
    ).join(EstateSell, EstateDeal.estate_sell_id == EstateSell.id).join(EstateHouse,
                                                                        EstateSell.house_id == EstateHouse.id).filter(
        EstateDeal.deal_status_name.in_(sold_statuses),
        effective_date.isnot(None),
        extract('year', effective_date) == year,
        extract('month', effective_date) == month,
        EstateSell.estate_sell_category == property_type
    ).group_by(EstateHouse.complex_name).all()
    return {row.complex_name: (row.fact_volume or 0) for row in results}


def get_plan_volume_data(year: int, month: int, property_type: str):
    """Получает плановые данные по объему контрактации из локальной SQLite."""
    results = g.company_db_session.query(planning_models.SalesPlan).filter_by(year=year, month=month,
                                                                              property_type=property_type).all()
    return {row.complex_name: row.plan_volume for row in results}


def generate_plan_fact_excel(year: int, month: int, property_type: str):
    """
    Генерирует Excel-файл с детальным план-фактным отчетом.
    """
    report_data, totals, total_refunds = generate_plan_fact_report(year, month, property_type)

    if not report_data:
        return None

    df = pd.DataFrame(report_data)
    totals_df = pd.DataFrame([totals])

    ordered_columns = [
        'complex_name',
        'plan_units', 'fact_units', 'percent_fact_units', 'forecast_units',
        'plan_volume', 'fact_volume', 'percent_fact_volume', 'forecast_volume',
        'plan_income', 'fact_income', 'percent_fact_income', 'expected_income'
    ]

    renamed_columns = [
        'Проект',
        'План, шт', 'Факт, шт', '% Факт, шт', '% Прогноз, шт',
        'План контрактации', 'Факт контрактации', '% Факт контр.', '% Прогноз контр.',
        'План поступлений', 'Факт поступлений', '% Факт поступл.', 'Ожидаемые поступл.'
    ]

    df['expected_income'] = df['expected_income'].apply(lambda x: x['sum'])
    df = df[ordered_columns]

    totals_df = totals_df[[col for col in ordered_columns if col != 'complex_name']]
    totals_df.insert(0, 'complex_name', f'Итого ({property_type})')

    final_df = pd.concat([df, totals_df], ignore_index=True)
    final_df.columns = renamed_columns

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        final_df.to_excel(writer, index=False, sheet_name=f'План-факт {month:02d}-{year}')

    output.seek(0)
    return output


def _get_yearly_fact_metrics_for_complex(year: int, complex_name: str, property_type: str = None):
    """
    Рассчитывает годовые фактические метрики (объем и поступления) из MySQL
    с разбивкой по месяцам для ОДНОГО ЖК.
    """
    house = g.mysql_db_session.query(EstateHouse).filter_by(complex_name=complex_name).first()
    if not house:
        return {'volume': [0] * 12, 'income': [0] * 12}

    fact_volume_by_month = [0] * 12
    fact_income_by_month = [0] * 12
    sold_statuses = ["Сделка в работе", "Сделка проведена"]

    effective_date = func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date)
    volume_query = g.mysql_db_session.query(
        extract('month', effective_date).label('month'),
        func.sum(EstateDeal.deal_sum).label('total')
    ).join(EstateSell).filter(
        EstateSell.house_id == house.id,
        EstateDeal.deal_status_name.in_(sold_statuses),
        effective_date.isnot(None),
        extract('year', effective_date) == year
    )
    if property_type:
        volume_query = volume_query.filter(EstateSell.estate_sell_category == property_type)

    for row in volume_query.group_by('month').all():
        fact_volume_by_month[row.month - 1] = row.total or 0

    income_query = g.mysql_db_session.query(
        extract('month', FinanceOperation.date_added).label('month'),
        func.sum(FinanceOperation.summa).label('total')
    ).join(EstateSell).filter(
        EstateSell.house_id == house.id,
        FinanceOperation.status_name == 'Проведено',
        extract('year', FinanceOperation.date_added) == year
    )
    if property_type:
        income_query = income_query.filter(EstateSell.estate_sell_category == property_type_system_name)

    for row in income_query.group_by('month').all():
        fact_income_by_month[row.month - 1] = row.total or 0

    return {'volume': fact_volume_by_month, 'income': fact_income_by_month}


def get_price_dynamics_data(complex_name: str, property_type: str = None):
    """
    Рассчитывает динамику средней фактической цены продажи за м² по месяцам из MySQL.
    """
    effective_date = func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date)
    # ИСПРАВЛЕНИЕ: Получаем статусы из настроек компании
    sold_statuses = current_user.company.sale_statuses

    query = g.mysql_db_session.query(
        extract('year', effective_date).label('deal_year'),
        extract('month', effective_date).label('deal_month'),
        (EstateDeal.deal_sum / EstateSell.estate_area).label('price_per_sqm')
    ).join(EstateSell, EstateDeal.estate_sell_id == EstateSell.id) \
        .join(EstateHouse, EstateSell.house_id == EstateHouse.id) \
        .filter(
        effective_date.isnot(None),
        EstateHouse.complex_name == complex_name,
        # ИСПРАВЛЕНИЕ: Используем динамический список статусов
        EstateDeal.deal_status_name.in_(sold_statuses),
        EstateSell.estate_area.isnot(None),
        EstateSell.estate_area > 0,
        EstateDeal.deal_sum.isnot(None),
        EstateDeal.deal_sum > 0
    )

    if property_type:
        query = query.filter(EstateSell.estate_sell_category == property_type)

    subquery = query.subquery()
    monthly_avg_query = g.mysql_db_session.query(
        subquery.c.deal_year,
        subquery.c.deal_month,
        func.avg(subquery.c.price_per_sqm).label('avg_price')
    ).group_by(subquery.c.deal_year, subquery.c.deal_month) \
        .order_by(subquery.c.deal_year, subquery.c.deal_month)

    results = monthly_avg_query.all()

    price_dynamics = {
        "labels": [],
        "data": []
    }
    for row in results:
        price_dynamics["labels"].append(f"{int(row.deal_month):02d}.{int(row.deal_year)}")
        price_dynamics["data"].append(row.avg_price)

    return price_dynamics


def calculate_grand_totals(year, month):
    """
    Рассчитывает общие итоговые показатели, включая ID для ссылок.
    """
    summary_by_type = get_monthly_summary_by_property_type(year, month)

    if not summary_by_type:
        return {}

    grand_totals = {
        'plan_units': sum(item.get('total_plan_units', 0) for item in summary_by_type),
        'fact_units': sum(item.get('total_fact_units', 0) for item in summary_by_type),
        'plan_volume': sum(item.get('total_plan_volume', 0) for item in summary_by_type),
        'fact_volume': sum(item.get('total_fact_volume', 0) for item in summary_by_type),
        'plan_income': sum(item.get('total_plan_income', 0) for item in summary_by_type),
        'fact_income': sum(item.get('total_fact_income', 0) for item in summary_by_type),
        'expected_income': sum(item['total_expected_income']['sum'] for item in summary_by_type),
        'expected_income_ids': [id_val for item in summary_by_type for id_val in item['total_expected_income']['ids']]
    }

    today = date.today()
    workdays_in_month = np.busday_count(f'{year}-{month:02d}-01',
                                        f'{year}-{month + 1:02d}-01' if month < 12 else f'{year + 1}-01-01')
    passed_workdays = np.busday_count(f'{year}-{month:02d}-01',
                                      today) if today.month == month and today.year == year else workdays_in_month
    passed_workdays = max(1, passed_workdays)

    grand_totals['percent_fact_units'] = (grand_totals['fact_units'] / grand_totals['plan_units'] * 100) if \
        grand_totals['plan_units'] > 0 else 0
    grand_totals['forecast_units'] = ((grand_totals['fact_units'] / passed_workdays) * workdays_in_month / grand_totals[
        'plan_units'] * 100) if grand_totals['plan_units'] > 0 else 0

    grand_totals['percent_fact_volume'] = (grand_totals['fact_volume'] / grand_totals['plan_volume'] * 100) if \
        grand_totals['plan_volume'] > 0 else 0
    grand_totals['forecast_volume'] = (
            (grand_totals['fact_volume'] / passed_workdays) * workdays_in_month / grand_totals[
        'plan_volume'] * 100) if grand_totals['plan_volume'] > 0 else 0

    grand_totals['percent_fact_income'] = (grand_totals['fact_income'] / grand_totals['plan_income'] * 100) if \
        grand_totals['plan_income'] > 0 else 0

    return grand_totals


# В файле app/services/report_service.py

def get_project_dashboard_data(complex_name: str, property_type: str = None):
    """Собирает данные для дашборда, комбинируя SQLite и MySQL."""
    print("\n" + "=" * 50)
    print(f"[DEBUG] 🏁 НАЧАЛО СБОРА ДАННЫХ ДЛЯ ДАШБОРДА: '{complex_name}'")

    today = date.today()
    sold_statuses = current_user.company.sale_statuses
    inventory_statuses = current_user.company.inventory_status_list

    # --- ЛОГ 1: Проверяем, какие статусы мы получили из настроек ---
    print(f"[DEBUG]  statuses -> sold: {sold_statuses}")
    print(f"[DEBUG] statuses -> inventory: {inventory_statuses}")

    prop_type_map = {member.value: member.name for member in planning_models.PropertyType}
    if not property_type:
        property_type = 'Квартира'
    property_type_system_name = prop_type_map.get(property_type)

    houses_in_complex = g.mysql_db_session.query(EstateHouse).filter_by(complex_name=complex_name).order_by(
        EstateHouse.name).all()
    active_version = g.company_db_session.query(planning_models.DiscountVersion).filter_by(is_active=True).first()

    houses_data = []
    for house in houses_in_complex:
        house_details = {"house_name": house.name, "property_types_data": {}}
        for prop_type_enum in planning_models.PropertyType:
            prop_type_value = prop_type_enum.value
            total_units = g.mysql_db_session.query(func.count(EstateSell.id)).filter(
                EstateSell.house_id == house.id, EstateSell.estate_sell_category == prop_type_enum.name
            ).scalar()
            if total_units == 0:
                continue

            sold_units = g.mysql_db_session.query(func.count(EstateDeal.id)).join(EstateSell).filter(
                EstateSell.house_id == house.id,
                EstateSell.estate_sell_category == prop_type_enum.name,
                EstateDeal.deal_status_name.in_(sold_statuses)
            ).scalar()

            remaining_count = total_units - sold_units
            avg_price_per_sqm = 0
            if remaining_count > 0 and active_version:
                discount = g.company_db_session.query(planning_models.Discount).filter_by(
                    version_id=active_version.id, complex_name=complex_name,
                    property_type=prop_type_enum, payment_method=planning_models.PaymentMethod.FULL_PAYMENT
                ).first()
                total_discount_rate = (discount.mpp or 0) + (discount.rop or 0) + (discount.kd or 0) if discount else 0

                unsold_units = g.mysql_db_session.query(EstateSell).filter(
                    EstateSell.house_id == house.id,
                    EstateSell.estate_sell_category == prop_type_enum.name,
                    EstateSell.estate_sell_status_name.in_(inventory_statuses)
                ).all()

                prices_per_sqm_list = []
                deduction_amount = 3_000_000 if prop_type_enum == planning_models.PropertyType.FLAT else 0
                for sell in unsold_units:
                    if sell.estate_price and sell.estate_price > deduction_amount and sell.estate_area and sell.estate_area > 0:
                        price_after_deduction = sell.estate_price - deduction_amount
                        final_price = price_after_deduction * (1 - total_discount_rate)
                        prices_per_sqm_list.append(final_price / sell.estate_area)
                if prices_per_sqm_list:
                    avg_price_per_sqm = sum(prices_per_sqm_list) / len(prices_per_sqm_list)

            house_details["property_types_data"][prop_type_value] = {
                "total_count": total_units, "remaining_count": remaining_count, "avg_price_per_sqm": avg_price_per_sqm
            }
        if house_details["property_types_data"]:
            houses_data.append(house_details)

    total_deals_volume = g.mysql_db_session.query(func.sum(EstateDeal.deal_sum)).join(EstateSell).join(
        EstateHouse).filter(
        EstateHouse.complex_name == complex_name, EstateDeal.deal_status_name.in_(sold_statuses)
    ).scalar() or 0
    total_income = g.mysql_db_session.query(func.sum(FinanceOperation.summa)).join(EstateSell).join(EstateHouse).filter(
        EstateHouse.complex_name == complex_name, FinanceOperation.status_name == 'Проведено'
    ).scalar() or 0

    remainders_by_type = {}
    print("[DEBUG] 🔄 Расчет KPI-карточек 'Стоимость остатков' и 'Осталось юнитов'...")
    for prop_type_enum in planning_models.PropertyType:
        prop_type_value = prop_type_enum.value

        print(f"[DEBUG]   - Ищем остатки для типа '{prop_type_value}' со статусами: {inventory_statuses}")

        remainder_sells_query = g.mysql_db_session.query(EstateSell).join(EstateHouse).filter(
            EstateHouse.complex_name == complex_name,
            EstateSell.estate_sell_category == prop_type_enum.name,
            EstateSell.estate_sell_status_name.in_(inventory_statuses)
        )

        unsold_objects = remainder_sells_query.all()
        print(f"[DEBUG]   - Найдено объектов в остатках: {len(unsold_objects)} шт.")

        if not unsold_objects:
            continue

        total_discount_rate = 0
        if active_version:
            discount = g.company_db_session.query(planning_models.Discount).filter_by(
                version_id=active_version.id,
                complex_name=complex_name,
                property_type=prop_type_enum,
                payment_method=planning_models.PaymentMethod.FULL_PAYMENT
            ).first()
            if discount:
                total_discount_rate = (discount.mpp or 0) + (discount.rop or 0) + (discount.kd or 0)

        total_discounted_price = 0
        count_remainder = 0
        deduction_amount = 3_000_000 if prop_type_enum == planning_models.PropertyType.FLAT else 0

        for sell in unsold_objects:
            if sell.estate_price and sell.estate_price > deduction_amount:
                price_after_deduction = sell.estate_price - deduction_amount
                final_price = price_after_deduction * (1 - total_discount_rate)
                total_discounted_price += final_price
                count_remainder += 1

        print(
            f"[DEBUG]   - Из них валидных для расчета: {count_remainder} шт. с общей стоимостью {total_discounted_price:,.0f} UZS")

        if count_remainder > 0:
            remainders_by_type[prop_type_value] = {
                'total_price': total_discounted_price,
                'count': count_remainder
            }

    print("[DEBUG] ✅ Расчет KPI-карточек завершен.")

    yearly_plan_fact = {
        'labels': [f"{i:02}" for i in range(1, 13)],
        'plan_volume': [0] * 12, 'fact_volume': [0] * 12,
        'plan_income': [0] * 12, 'fact_income': [0] * 12
    }

    plans_query = g.company_db_session.query(planning_models.SalesPlan).filter_by(complex_name=complex_name,
                                                                                  year=today.year)
    if property_type:
        plans_query = plans_query.filter_by(property_type=property_type)
    for p in plans_query.all():
        yearly_plan_fact['plan_volume'][p.month - 1] += p.plan_volume
        yearly_plan_fact['plan_income'][p.month - 1] += p.plan_income

    fact_volume_by_month = [0] * 12
    effective_date = func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date)
    volume_query = g.mysql_db_session.query(
        extract('month', effective_date).label('month'),
        func.sum(EstateDeal.deal_sum).label('total')
    ).join(EstateSell).join(EstateHouse).filter(
        EstateHouse.complex_name == complex_name,
        EstateDeal.deal_status_name.in_(sold_statuses),
        effective_date.isnot(None),
        extract('year', effective_date) == today.year
    )
    if property_type_system_name:
        volume_query = volume_query.filter(EstateSell.estate_sell_category == property_type_system_name)
    for row in volume_query.group_by('month').all():
        fact_volume_by_month[row.month - 1] = row.total or 0
    yearly_plan_fact['fact_volume'] = fact_volume_by_month

    fact_income_by_month = [0] * 12
    income_query = g.mysql_db_session.query(
        extract('month', FinanceOperation.date_added).label('month'),
        func.sum(FinanceOperation.summa).label('total')
    ).join(EstateSell).join(EstateHouse).filter(
        EstateHouse.complex_name == complex_name,
        FinanceOperation.status_name == 'Проведено',
        extract('year', FinanceOperation.date_added) == today.year
    )
    if property_type_system_name:
        income_query = income_query.filter(EstateSell.estate_sell_category == property_type_system_name)
    for row in income_query.group_by('month').all():
        fact_income_by_month[row.month - 1] = row.total or 0
    yearly_plan_fact['fact_income'] = fact_income_by_month

    recent_deals = g.mysql_db_session.query(
        EstateDeal.id, EstateDeal.deal_sum, EstateSell.estate_sell_category.label('property_type'),
        func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date).label('deal_date')
    ).join(EstateSell).join(EstateHouse).filter(
        EstateHouse.complex_name == complex_name,
        EstateDeal.deal_status_name.in_(sold_statuses)
    ).order_by(
        func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date).desc()
    ).limit(15).all()

    remainders_chart_data = {"labels": [], "data": []}
    if remainders_by_type:
        remainders_chart_data["labels"] = list(remainders_by_type.keys())
        remainders_chart_data["data"] = [v['count'] for v in remainders_by_type.values()]

    sales_analysis = {"by_floor": {}, "by_rooms": {}, "by_area": {}}
    type_to_analyze = property_type if property_type else 'Квартира'

    if type_to_analyze == 'Квартира':
        base_query = g.mysql_db_session.query(EstateSell).join(EstateDeal).join(EstateHouse).filter(
            EstateHouse.complex_name == complex_name,
            EstateDeal.deal_status_name.in_(sold_statuses),
            EstateSell.estate_sell_category == 'FLAT'
        )

        floor_data = base_query.with_entities(EstateSell.estate_floor, func.count(EstateSell.id)).group_by(
            EstateSell.estate_floor).order_by(EstateSell.estate_floor).all()
        if floor_data:
            sales_analysis['by_floor']['labels'] = [f"{row[0]} этаж" for row in floor_data if row[0] is not None]
            sales_analysis['by_floor']['data'] = [row[1] for row in floor_data if row[0] is not None]

        rooms_data = base_query.filter(EstateSell.estate_rooms.isnot(None)).with_entities(EstateSell.estate_rooms,
                                                                                          func.count(
                                                                                              EstateSell.id)).group_by(
            EstateSell.estate_rooms).order_by(EstateSell.estate_rooms).all()
        if rooms_data:
            sales_analysis['by_rooms']['labels'] = [f"{int(row[0])}-комн." for row in rooms_data if row[0] is not None]
            sales_analysis['by_rooms']['data'] = [row[1] for row in rooms_data if row[0] is not None]

        area_case = case(
            (EstateSell.estate_area < 40, "до 40 м²"), (EstateSell.estate_area.between(40, 50), "40-50 м²"),
            (EstateSell.estate_area.between(50, 60), "50-60 м²"), (EstateSell.estate_area.between(60, 75), "60-75 м²"),
            (EstateSell.estate_area.between(75, 90), "75-90 м²"), (EstateSell.estate_area >= 90, "90+ м²"),
        )
        area_data = base_query.filter(EstateSell.estate_area.isnot(None)).with_entities(area_case, func.count(
            EstateSell.id)).group_by(area_case).order_by(area_case).all()
        if area_data:
            sales_analysis['by_area']['labels'] = [row[0] for row in area_data if row[0] is not None]
            sales_analysis['by_area']['data'] = [row[1] for row in area_data if row[0] is not None]

    dashboard_data = {
        "complex_name": complex_name,
        "kpi": {"total_deals_volume": total_deals_volume, "total_income": total_income,
                "remainders_by_type": remainders_by_type},
        "charts": {
            "plan_fact_dynamics_yearly": yearly_plan_fact,
            "remainders_chart_data": remainders_chart_data,
            "sales_analysis": sales_analysis,
            "price_dynamics": get_price_dynamics_data(complex_name, property_type_system_name)
        },
        "recent_deals": recent_deals,
        "houses_data": houses_data,
    }

    print(f"[DEBUG] ✅ ФИНАЛЬНЫЕ ДАННЫЕ ДЛЯ KPI: {json.dumps(remainders_by_type)}")
    print("=" * 50 + "\n")
    return dashboard_data