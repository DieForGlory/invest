# app/services/manager_report_service.py
import openpyxl
from sqlalchemy import or_
import pandas as pd
import re
from datetime import datetime, date
from collections import defaultdict
from sqlalchemy import func, extract
import io
from flask import g
from app.core.extensions import db
from ..core.db_utils import require_mysql_db
from flask_login import current_user

# Обновленные импорты
from app.models import auth_models
from app.models import planning_models
from app.models.estate_models import EstateDeal, EstateSell, EstateHouse
from app.models.finance_models import FinanceOperation
from ..models import planning_models
from . import currency_service


@require_mysql_db
def process_manager_plans_from_excel(file_path: str):
    """
    Обрабатывает Excel-файл с персональными планами менеджеров.
    """
    df = pd.read_excel(file_path)
    plans_to_save = defaultdict(lambda: defaultdict(float))
    # В регулярном выражении оставляем только "поступления"
    header_pattern = re.compile(r"(поступления) (\d{2}\.\d{2}\.\d{4})", re.IGNORECASE)

    # ИСПРАВЛЕНО: Используем auth_models.SalesManager для поиска менеджеров в таблице 'users'
    managers_map = {m.users_name: m.id for m in g.mysql_db_session.query(auth_models.SalesManager).filter.all()}

    for index, row in df.iterrows():
        manager_name = row.iloc[0]
        if manager_name not in managers_map:
            print(f"[MANAGER PLANS] ⚠️ ВНИМАНИЕ: Менеджер '{manager_name}' не найден в базе. Строка пропущена.")
            continue
        manager_id = managers_map[manager_name]

        for col_name, value in row.iloc[1:].items():
            if pd.isna(value) or value == 0:
                continue
            match = header_pattern.search(str(col_name))
            if not match:
                continue

            # Логика упрощена, так как у нас только один тип плана
            plan_type_str = match.group(1)
            date_str = match.group(2)
            plan_date = datetime.strptime(date_str, '%d.%m.%Y')
            year, month = plan_date.year, plan_date.month

            if 'поступления' in plan_type_str.lower():
                plans_to_save[(manager_id, year, month)]['plan_income'] += float(value)

    updated_count, created_count = 0, 0
    for (manager_id, year, month), values in plans_to_save.items():
        # ИСПРАВЛЕНО: Используем g.company_db_session для записи в локальную базу
        plan_entry = g.company_db_session.query(planning_models.ManagerSalesPlan).filter_by(
            manager_id=manager_id, year=year, month=month
        ).first()
        if not plan_entry:
            plan_entry = planning_models.ManagerSalesPlan(manager_id=manager_id, year=year, month=month)
            g.company_db_session.add(plan_entry)
            created_count += 1
        else:
            updated_count += 1

        # Устанавливаем plan_volume в 0, обновляем только plan_income
        plan_entry.plan_volume = 0.0
        plan_entry.plan_income = values.get('plan_income', 0.0)

    g.company_db_session.commit()
    return f"Успешно обработано планов: создано {created_count}, обновлено {updated_count}."


@require_mysql_db
def get_manager_performance_details(manager_id: int, year: int):
    """
    Собирает детальную информацию по выполнению плана для одного менеджера за год,
    ЗАРАНЕЕ РАССЧИТЫВАЯ KPI ДЛЯ КАЖДОГО МЕСЯЦА. (С ОТЛАДКОЙ)
    """
    print("\n" + "=" * 50)
    print(f"[MANAGER_PERFORMANCE] 🏁 Старт сбора данных для менеджера ID: {manager_id}, Год: {year}")

    sold_statuses = current_user.company.sale_statuses
    print(f"[MANAGER_PERFORMANCE] ✅ Используются статусы для ФАКТА ПРОДАЖ: {sold_statuses}")

    # ИСПРАВЛЕНО: Используем auth_models.SalesManager для поиска менеджера
    manager = g.mysql_db_session.query(auth_models.SalesManager).filter(
        auth_models.SalesManager.id == manager_id
    ).first()
    if not manager:
        print(f"[MANAGER_PERFORMANCE] ❌ Менеджер с ID {manager_id} не найден в базе MySQL.")
        print("=" * 50 + "\n")
        return None
    print(f"[MANAGER_PERFORMANCE] ✅ Найден менеджер: {manager.users_name}")

    # ИСПРАВЛЕНО: Используем g.company_db_session для чтения планов
    plans_query = g.company_db_session.query(planning_models.ManagerSalesPlan).filter_by(manager_id=manager_id,
                                                                                         year=year).all()
    plan_data = {p.month: p for p in plans_query}
    print(f"[MANAGER_PERFORMANCE] 📚 Найдены планы для {len(plan_data)} месяцев в локальной базе.")

    effective_date = func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date)
    fact_volume_query = g.mysql_db_session.query(
        extract('month', effective_date).label('month'),
        func.sum(EstateDeal.deal_sum).label('fact_volume')
    ).filter(
        EstateDeal.deal_manager_id == manager_id,
        extract('year', effective_date) == year,
        EstateDeal.deal_status_name.in_(sold_statuses)
    ).group_by('month').all()
    print(f"[MANAGER_PERFORMANCE] 📥 SQL-запрос по ФАКТУ ОБЪЕМА ВЕРНУЛ {len(fact_volume_query)} строк.")
    fact_volume_data = {row.month: row.fact_volume or 0 for row in fact_volume_query}
    print(f"[MANAGER_PERFORMANCE] 👉 Обработанные данные по ФАКТУ ОБЪЕМА: {fact_volume_data}")

    fact_income_query = g.mysql_db_session.query(
        extract('month', FinanceOperation.date_added).label('month'),
        func.sum(FinanceOperation.summa).label('fact_income')
    ).filter(
        FinanceOperation.manager_id == manager_id,
        extract('year', FinanceOperation.date_added) == year,
        FinanceOperation.status_name == "Paid",
        or_(
            FinanceOperation.payment_type != "Возврат поступлений при отмене сделки",
            FinanceOperation.payment_type.is_(None)
        )
    ).group_by('month').all()
    print(f"[MANAGER_PERFORMANCE] 📥 SQL-запрос по ФАКТУ ПОСТУПЛЕНИЙ ВЕРНУЛ {len(fact_income_query)} строк.")
    fact_income_data = {row.month: row.fact_income or 0 for row in fact_income_query}
    print(f"[MANAGER_PERFORMANCE] 👉 Обработанные данные по ФАКТУ ПОСТУПЛЕНИЙ: {fact_income_data}")

    report = []
    print("[MANAGER_PERFORMANCE] 🔄 Начало формирования отчета по месяцам...")
    for month_num in range(1, 13):
        plan = plan_data.get(month_num)
        fact_volume = fact_volume_data.get(month_num, 0)
        fact_income = fact_income_data.get(month_num, 0)
        plan_income = plan.plan_income if plan else 0.0

        kpi_bonus = calculate_manager_kpi(plan_income, fact_income)

        # Логируем данные за каждый месяц
        print(
            f"  [Месяц {month_num:02d}] План поступлений: {plan_income}, Факт поступлений: {fact_income}, Факт объем: {fact_volume}")

        report.append({
            'month': month_num,
            'plan_volume': plan.plan_volume if plan else 0,
            'fact_volume': fact_volume,
            'volume_percent': (fact_volume / plan.plan_volume * 100) if (plan and plan.plan_volume > 0) else 0,
            'plan_income': plan_income,
            'fact_income': fact_income,
            'income_percent': (fact_income / plan_income * 100) if (plan and plan_income > 0) else 0,
            'kpi_bonus': kpi_bonus
        })

    final_report = {'manager_id': manager_id, 'manager_name': manager.users_name, 'performance': report}
    print("[MANAGER_PERFORMANCE] ✅ Отчет успешно сформирован.")
    print("=" * 50 + "\n")

    # ИСПРАВЛЕНО: Обращаемся к правильному полю users_name
    return final_report


@require_mysql_db
def generate_manager_plan_template_excel():
    """
    Генерирует Excel-файл с ФИО всех менеджеров и столбцами планов на текущий год.
    """
    # ИСПРАВЛЕНО: Используем auth_models.SalesManager для получения списка менеджеров
    managers = g.mysql_db_session.query(auth_models.SalesManager).order_by(auth_models.SalesManager.users_name).all()
    manager_names = [manager.users_name for manager in managers]

    current_year = date.today().year
    headers = ['ФИО']
    # В цикле убираем добавление столбца "Контрактация"
    for month in range(1, 13):
        date_str = f"01.{month:02d}.{current_year}"
        headers.append(f"Поступления {date_str}")

    data = [{'ФИО': name, **{header: 0 for header in headers[1:]}} for name in manager_names]

    df = pd.DataFrame(data, columns=headers)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Шаблон планов')
        worksheet = writer.sheets['Шаблон планов']
        worksheet.column_dimensions['A'].width = 35
        for i in range(1, len(headers)):
            col_letter = openpyxl.utils.get_column_letter(i + 1)
            worksheet.column_dimensions[col_letter].width = 25
    output.seek(0)
    return output


def calculate_manager_kpi(plan_income: float, fact_income: float) -> float:
    if not plan_income or plan_income == 0:
        return 0.0

    completion_percentage = (fact_income / plan_income) * 100

    if completion_percentage >= 100:
        bonus = fact_income * 0.005
    elif completion_percentage >= 90:
        bonus = fact_income * 0.004
    elif completion_percentage >= 80:
        bonus = fact_income * 0.003
    else:
        bonus = 0.0

    return bonus


@require_mysql_db
def generate_kpi_report_excel(year: int, month: int):
    """
    Создает детализированный и отформатированный отчет по KPI менеджеров в формате Excel.
    """
    usd_rate = currency_service.get_current_effective_rate()
    if not usd_rate or usd_rate == 0:
        raise ValueError("Не удалось получить актуальный курс USD.")

    # ИСПРАВЛЕНО: Используем g.company_db_session для чтения планов
    plans = g.company_db_session.query(planning_models.ManagerSalesPlan).filter(
        planning_models.ManagerSalesPlan.year == year,
        planning_models.ManagerSalesPlan.month == month,
        planning_models.ManagerSalesPlan.plan_income > 0
    ).all()

    if not plans:
        return None

    manager_ids_with_plans = [p.manager_id for p in plans]
    plans_map = {p.manager_id: p for p in plans}

    # ИСПРАВЛЕНО: Используем auth_models.SalesManager для получения ФИО и должности
    managers = g.mysql_db_session.query(auth_models.SalesManager).filter(
        auth_models.SalesManager.id.in_(manager_ids_with_plans)
    ).order_by(auth_models.SalesManager.users_name).all()

    source_data = []
    for manager in managers:
        plan = plans_map.get(manager.id)
        if not plan:
            continue

        fact_income_query = g.mysql_db_session.query(
            func.sum(FinanceOperation.summa)
        ).filter(
            FinanceOperation.manager_id == manager.id,
            extract('year', FinanceOperation.date_added) == year,
            extract('month', FinanceOperation.date_added) == month,
            FinanceOperation.status_name == "Paid",
            or_(
                FinanceOperation.payment_type != "Возврат поступлений при отмене сделки",
                FinanceOperation.payment_type.is_(None)
            )
        ).scalar()
        fact_income = fact_income_query or 0.0
        kpi_bonus_uzs = calculate_manager_kpi(plan.plan_income, fact_income)

        source_data.append({
            "full_name": manager.users_name,  # ИСПРАВЛЕНО
            "position": manager.post_title or 'Менеджер по продажам',  # ИСПРАВЛЕНО
            "plan_uzs": plan.plan_income,
            "fact_uzs": fact_income,
            "kpi_bonus_uzs": kpi_bonus_uzs,
            "kpi_bonus_usd": kpi_bonus_uzs / usd_rate
        })

    final_report_rows = []
    for i, data in enumerate(source_data):
        final_report_rows.append({
            '№': i + 1,
            'ФИО менеджера': data['full_name'],
            'Должность': data['position'],
            'Личный план продаж на период (долл. США)': data['plan_uzs'] / usd_rate,
            'Факт выполнения личного плана продаж на период (долл. США)': data['fact_uzs'] / usd_rate,
            '% выполнения личного плана продаж': (data['fact_uzs'] / data['plan_uzs']) if data['plan_uzs'] > 0 else 0,
            'Удовлетворенность работой сотрудника (коэф.)': None,
            'Итоговая сумма к выплате, NET (долл. США)': None,
            'Итоговая сумма к выплате, NET (сум)': None,
            'Итоговая сумма к выплате, GROSS (сум)': None
        })

    df = pd.DataFrame(final_report_rows)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Ведомость KPI', index=False, startrow=1)
        workbook = writer.book
        worksheet = writer.sheets['Ведомость KPI']
        header_format = workbook.add_format(
            {'bold': True, 'text_wrap': True, 'valign': 'top', 'fg_color': '#D7E4BC', 'border': 1, 'align': 'center'})
        money_usd_format = workbook.add_format({'num_format': '$#,##0.00', 'border': 1})
        money_uzs_format = workbook.add_format({'num_format': '#,##0', 'border': 1})
        percent_format = workbook.add_format({'num_format': '0.0%', 'border': 1})
        coef_format = workbook.add_format({'bg_color': '#FFFFCC', 'border': 1})
        title_format = workbook.add_format({'bold': True, 'font_size': 14, 'align': 'center'})
        month_names = {1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь', 7: 'Июль', 8: 'Август',
                       9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'}
        worksheet.merge_range('A1:J1', f'Ведомость по KPI за {month_names.get(month, "")} {year}', title_format)
        for col_num, value in enumerate(df.columns):
            worksheet.write(1, col_num, value, header_format)
        worksheet.set_column('A:A', 5)
        worksheet.set_column('B:B', 35)
        worksheet.set_column('C:C', 25)
        worksheet.set_column('D:E', 20, money_usd_format)
        worksheet.set_column('F:F', 15, percent_format)
        worksheet.set_column('G:G', 25, coef_format)
        worksheet.set_column('H:H', 25, money_usd_format)
        worksheet.set_column('I:I', 25, money_uzs_format)
        worksheet.set_column('J:J', 25, money_uzs_format)
        for idx, data in enumerate(source_data):
            row_num = idx + 3
            kpi_usd = data['kpi_bonus_usd']
            kpi_uzs = data['kpi_bonus_uzs']
            worksheet.write_formula(f'H{row_num}', f'=IF(ISBLANK(G{row_num}),0,{kpi_usd}*G{row_num})')
            worksheet.write_formula(f'I{row_num}', f'=IF(ISBLANK(G{row_num}),0,{kpi_uzs}*G{row_num})')
            worksheet.write_formula(f'J{row_num}', f'=IF(ISBLANK(I{row_num}),0,I{row_num}/0.88)')
    output.seek(0)
    return output


@require_mysql_db
def get_manager_kpis(manager_id: int, year: int):
    sold_statuses = current_user.company.sale_statuses
    """
    Рассчитывает расширенные KPI для одного менеджера на основе ПОСТУПЛЕНИЙ.
    """
    best_complex_query = g.mysql_db_session.query(
        EstateHouse.complex_name, func.count(EstateDeal.id).label('deal_count')
    ).join(EstateSell, EstateHouse.sells).join(EstateDeal, EstateSell.deals) \
        .filter(
        EstateDeal.deal_manager_id == manager_id,
        EstateDeal.deal_status_name.in_(sold_statuses)
    ).group_by(EstateHouse.complex_name).order_by(func.count(EstateDeal.id).desc()).first()

    units_by_type_query = g.mysql_db_session.query(
        EstateSell.estate_sell_category, func.count(EstateDeal.id).label('unit_count')
    ).join(EstateDeal, EstateSell.deals).filter(
        EstateDeal.deal_manager_id == manager_id,
        EstateDeal.deal_status_name.in_(sold_statuses)
    ).group_by(EstateSell.estate_sell_category).all()

    best_year_income_query = g.mysql_db_session.query(
        extract('year', FinanceOperation.date_added).label('income_year'),
        func.sum(FinanceOperation.summa).label('total_income')
    ).filter(
        FinanceOperation.manager_id == manager_id,
        FinanceOperation.status_name == 'Paid'
    ).group_by('income_year').order_by(func.sum(FinanceOperation.summa).desc()).first()

    best_month_income_query = g.mysql_db_session.query(
        extract('year', FinanceOperation.date_added).label('income_year'),
        extract('month', FinanceOperation.date_added).label('income_month'),
        func.sum(FinanceOperation.summa).label('total_income')
    ).filter(
        FinanceOperation.manager_id == manager_id,
        FinanceOperation.status_name == 'Paid'
    ).group_by('income_year', 'income_month').order_by(func.sum(FinanceOperation.summa).desc()).first()

    best_month_in_year_income_query = g.mysql_db_session.query(
        extract('month', FinanceOperation.date_added).label('income_month'),
        func.sum(FinanceOperation.summa).label('total_income')
    ).filter(
        FinanceOperation.manager_id == manager_id,
        extract('year', FinanceOperation.date_added) == year,
        FinanceOperation.status_name == 'Paid'
    ).group_by('income_month').order_by(func.sum(FinanceOperation.summa).desc()).first()

    kpis = {
        'best_complex': {
            'name': best_complex_query.complex_name if best_complex_query else None,
            'count': best_complex_query.deal_count if best_complex_query else 0
        },
        'units_by_type': {row.estate_sell_category: row.unit_count for row in units_by_type_query},
        'best_month_in_year': {
            'income': {
                'month': int(best_month_in_year_income_query.income_month) if best_month_in_year_income_query else 0,
                'total': best_month_in_year_income_query.total_income if best_month_in_year_income_query else 0
            }
        },
        'all_time_records': {
            'best_year_income': {
                'year': int(best_year_income_query.income_year) if best_year_income_query else 0,
                'total': best_year_income_query.total_income if best_year_income_query else 0
            },
            'best_month_income': {
                'year': int(best_month_income_query.income_year) if best_month_income_query else 0,
                'month': int(best_month_income_query.income_month) if best_month_income_query else 0,
                'total': best_month_income_query.total_income if best_month_income_query else 0
            }
        }
    }
    return kpis


@require_mysql_db
def get_manager_complex_ranking(manager_id: int):
    """
    Возвращает рейтинг ЖК по количеству сделок и объему ПОСТУПЛЕНИЙ для менеджера.
    """
    # ИСПРАВЛЕНО: Используем g.mysql_db_session
    ranking = g.mysql_db_session.query(
        EstateHouse.complex_name,
        func.sum(FinanceOperation.summa).label('total_income'),
        func.count(func.distinct(EstateDeal.id)).label('deal_count')
    ).join(EstateSell, EstateHouse.id == EstateSell.house_id) \
        .join(EstateDeal, EstateSell.id == EstateDeal.estate_sell_id) \
        .join(FinanceOperation, EstateSell.id == FinanceOperation.estate_sell_id) \
        .filter(
        EstateDeal.deal_manager_id == manager_id,
        FinanceOperation.manager_id == manager_id,
        FinanceOperation.status_name == "Paid"
    ) \
        .group_by(EstateHouse.complex_name) \
        .order_by(func.sum(FinanceOperation.summa).desc()) \
        .all()
    return [{"name": r.complex_name, "total_income": r.total_income, "deal_count": r.deal_count} for r in ranking]


@require_mysql_db
def get_complex_hall_of_fame(complex_name: str, start_date_str: str = None, end_date_str: str = None):
    sold_statuses = current_user.company.sale_statuses
    """
    Возвращает рейтинг менеджеров по количеству и объему сделок для ЖК.
    """
    sold_statuses = ["Сделка в работе", "Сделка проведена"]
    # ИСПРАВЛЕНО: Используем g.mysql_db_session и auth_models.SalesManager
    query = g.mysql_db_session.query(
        auth_models.SalesManager.users_name,
        func.count(EstateDeal.id).label('deal_count'),
        func.sum(EstateDeal.deal_sum).label('total_volume'),
        func.sum(EstateSell.estate_area).label('total_area')
    ).join(EstateDeal, auth_models.SalesManager.id == EstateDeal.deal_manager_id) \
        .join(EstateSell, EstateDeal.estate_sell_id == EstateSell.id) \
        .join(EstateHouse, EstateSell.house_id == EstateHouse.id) \
        .filter(
        EstateHouse.complex_name == complex_name,
        EstateDeal.deal_status_name.in_(sold_statuses)
    )

    if start_date_str:
        start_date = date.fromisoformat(start_date_str)
        query = query.filter(func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date) >= start_date)
    if end_date_str:
        end_date = date.fromisoformat(end_date_str)
        query = query.filter(func.coalesce(EstateDeal.agreement_date, EstateDeal.preliminary_date) <= end_date)

    ranking = query.group_by(auth_models.SalesManager.id).order_by(func.count(EstateDeal.id).desc()).all()
    # ИСПРАВЛЕНО: Обращаемся к правильному полю users_name
    return [{'full_name': r.users_name, 'deal_count': r.deal_count, 'total_volume': r.total_volume,
             'total_area': r.total_area} for r in ranking]