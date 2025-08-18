# app/web/main_routes.py

import json
from datetime import datetime
from flask import session
from flask import Blueprint, render_template, request, flash, redirect, url_for, current_app
from flask import abort
from flask_login import login_required, current_user
from flask_babel import gettext as _
from app.services import special_offer_service
from ..core.decorators import permission_required
from ..core.extensions import db
from ..models import auth_models
from ..models.estate_models import EstateHouse
from ..models.exclusion_models import ExcludedSell
# --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
# Импортируем PropertyType и PaymentMethod из их нового местоположения
from ..models.planning_models import PropertyType, PaymentMethod
from ..services import settings_service
from ..services.data_service import get_sells_with_house_info, get_filter_options
from ..services.discount_service import get_current_usd_rate
from ..services.selection_service import find_apartments_by_budget, get_apartment_card_data

main_bp = Blueprint('main', __name__, template_folder='templates')

@main_bp.route('/language/<lang>')
def set_language(lang=None):
    session['language'] = lang
    return redirect(request.referrer)

@main_bp.route('/show-all-routes')
@login_required
def show_all_routes():
    """Временная страница для отображения всех зарегистрированных маршрутов."""
    rules = []
    for rule in current_app.url_map.iter_rules():
        rules.append(f"Endpoint: {rule.endpoint}, Path: {rule.rule}, Methods: {','.join(rule.methods)}")

    rules.sort()

    response_html = "<h1>Зарегистрированные URL-адреса</h1><ul>"
    for r in rules:
        # Выделим жирным маршруты нашего проблемного модуля
        if 'special_offer' in r:
            response_html += f"<li><strong>{r}</strong></li>"
        else:
            response_html += f"<li>{r}</li>"
    response_html += "</ul>"

    return response_html

@main_bp.route('/search-by-id', methods=['POST'])
@login_required
@permission_required('view_selection')
def search_by_id():
    sell_id = request.form.get('search_id')
    if sell_id:
        try:
            int(sell_id)
            return redirect(url_for('main.apartment_details', sell_id=sell_id))
        except ValueError:
            flash('Пожалуйста, введите корректный числовой ID.', 'warning')
            return redirect(url_for('main.selection'))
    else:
        flash('Вы не ввели ID для поиска.', 'info')
        return redirect(url_for('main.selection'))


@main_bp.route('/')
@login_required
@permission_required('view_selection')
def index():
    page = request.args.get('page', 1, type=int)
    PER_PAGE = 40
    sells_pagination = get_sells_with_house_info(page=page, per_page=PER_PAGE)

    if not sells_pagination:
        flash("Не удалось загрузить данные о продажах.", "danger")
        return render_template('main/index.html', title='Ошибка', sells_pagination=None)

    return render_template('main/index.html', title='Главная', sells_pagination=sells_pagination)


@main_bp.route('/selection', methods=['GET', 'POST'])
@login_required
@permission_required('view_selection')
def selection():
    results = None
    filter_options = get_filter_options()

    # --- НАЧАЛО ИЗМЕНЕНИЙ ---

    # Создаем переведенные списки для передачи в шаблон
    translated_property_types = [
        {'value': pt.value, 'display': _(pt.value)} for pt in PropertyType
    ]
    translated_payment_methods = [
        # Экранируем '%' для gettext
        {'value': pm.value, 'display': _(pm.value.replace('%', '%%'))} for pm in PaymentMethod
    ]

    if request.method == 'POST':
        try:
            budget = float(request.form.get('budget'))
            currency = request.form.get('currency')
            prop_type_str = request.form.get('property_type')
            floor = request.form.get('floor')
            rooms = request.form.get('rooms')
            payment_method = request.form.get('payment_method')

            results = find_apartments_by_budget(
                budget,
                currency,
                prop_type_str,
                floor=floor,
                rooms=rooms,
                payment_method=payment_method
            )
        except (ValueError, TypeError):
            flash("Пожалуйста, введите корректную сумму бюджета.", "danger")

    return render_template('main/selection.html',
                           title=_("Подбор по бюджету"),  # <-- Тоже переводим заголовок
                           results=results,
                           # --- НАЧАЛО ИЗМЕНЕНИЙ: Передаем новые списки в шаблон ---
                           property_types=translated_property_types,
                           payment_methods=translated_payment_methods,
                           # --- КОНЕЦ ИЗМЕНЕНИЙ ---
                           filter_options=filter_options)


@main_bp.route('/apartment/<int:sell_id>')
@login_required
@permission_required('view_selection')
def apartment_details(sell_id):
    card_data = get_apartment_card_data(sell_id)
    all_discounts_data = card_data.pop('all_discounts_for_property_type', [])

    return render_template(
        'main/apartment_details.html',
        data=card_data,
        all_discounts_for_property_type=all_discounts_data,
        title=f"Детали объекта ID {sell_id}"
    )


@main_bp.route('/commercial-offer/<int:sell_id>')
@login_required
@permission_required('view_selection')
def generate_commercial_offer(sell_id):
    card_data = get_apartment_card_data(sell_id)
    if not card_data.get('apartment'):
        return "Apartment not found", 404

    selections_json = request.args.get('selections', '{}')
    try:
        user_selections = json.loads(selections_json)
    except json.JSONDecodeError:
        user_selections = {}

    updated_pricing_for_template = []
    base_options = card_data.get('pricing', [])
    all_discounts = card_data.get('all_discounts_for_property_type', [])

    for option in base_options:
        type_key = option['type_key']
        base_price_deducted = option['price_after_deduction']
        total_discount_rate = sum(d['value'] for d in option.get('discounts', []))
        applied_discounts_details = [{'name': disc['name'], 'amount': base_price_deducted * disc['value']} for disc in option.get('discounts', [])]

        base_payment_method_value = '100% оплата' if '100' in type_key or 'full_payment' in type_key else 'Ипотека'
        discount_object = next((d for d in all_discounts if d['payment_method'] == base_payment_method_value), None)

        if type_key in user_selections and discount_object:
            for disc_name, disc_percent in user_selections[type_key].items():
                server_percent = discount_object.get(disc_name, 0) * 100
                if abs(float(disc_percent) - server_percent) < 0.01:
                    rate_to_add = server_percent / 100.0
                    total_discount_rate += rate_to_add
                    applied_discounts_details.append({'name': disc_name.upper(), 'amount': base_price_deducted * rate_to_add})

        final_price = base_price_deducted * (1 - total_discount_rate)
        initial_payment = None

        if 'mortgage' in type_key:
            from ..services.selection_service import MAX_MORTGAGE, MIN_INITIAL_PAYMENT_PERCENT
            initial_payment = max(0, final_price - MAX_MORTGAGE)
            min_req_payment = final_price * MIN_INITIAL_PAYMENT_PERCENT
            if initial_payment < min_req_payment:
                initial_payment = min_req_payment
            final_price = initial_payment + MAX_MORTGAGE

        option.update({
            'final_price': final_price,
            'initial_payment': initial_payment,
            'applied_discounts': applied_discounts_details
        })
        updated_pricing_for_template.append(option)

    card_data['pricing'] = updated_pricing_for_template

    current_date = datetime.now().strftime("%d.%m.%Y %H:%M")
    usd_rate_from_cbu = get_current_usd_rate()
    fallback_usd_rate = current_app.config.get('USD_TO_UZS_RATE', 12650.0)
    actual_usd_rate = usd_rate_from_cbu if usd_rate_from_cbu is not None else fallback_usd_rate

    return render_template(
        'main/commercial_offer.html',
        data=card_data,
        current_date=current_date,
        usd_to_uzs_rate=actual_usd_rate,
        title=f"КП по объекту ID {sell_id}"
    )


@main_bp.route('/exclusions', methods=['GET', 'POST'])
@login_required
@permission_required('manage_settings')
def manage_exclusions():
    if request.method == 'POST':
        if 'sell_id_to_manage' in request.form:
            action = request.form.get('action')
            sell_id_str = request.form.get('sell_id_to_manage')
            comment = request.form.get('comment', '').strip()

            if not sell_id_str:
                flash("ID квартиры не может быть пустым.", "danger")
            else:
                try:
                    sell_id = int(sell_id_str)
                    if action == 'add':
                        if ExcludedSell.query.filter_by(sell_id=sell_id).first():
                            flash(f"Квартира с ID {sell_id} уже в исключениях.", "warning")
                        else:
                            db.session.add(ExcludedSell(sell_id=sell_id, comment=comment or None))
                            db.session.commit()
                            flash(f"Квартира ID {sell_id} добавлена в исключения.", "success")
                    elif action == 'delete':
                        exclusion = ExcludedSell.query.filter_by(sell_id=sell_id).first()
                        if exclusion:
                            db.session.delete(exclusion)
                            db.session.commit()
                            flash(f"Квартира ID {sell_id} удалена из исключений.", "success")
                except ValueError:
                    flash("ID квартиры должен быть числом.", "danger")

        elif 'complex_name_to_toggle' in request.form:
            complex_name = request.form.get('complex_name_to_toggle')
            if complex_name:
                message, category = settings_service.toggle_complex_exclusion(complex_name)
                flash(message, category)

        return redirect(url_for('main.manage_exclusions'))

    excluded_sells = ExcludedSell.query.order_by(ExcludedSell.created_at.desc()).all()
    all_complexes = db.session.query(EstateHouse.complex_name).distinct().order_by(EstateHouse.complex_name).all()
    excluded_complexes_names = {c.complex_name for c in settings_service.get_all_excluded_complexes()}

    return render_template(
        'settings/manage_exclusions.html',
        title="Управление исключениями",
        excluded_sells=excluded_sells,
        all_complexes=[c[0] for c in all_complexes],
        excluded_complex_names=excluded_complexes_names
    )


@main_bp.route('/monthly-specials')
@login_required
def monthly_specials_list():
    """Отображает галерею активных квартир месяца."""
    active_offers = special_offer_service.get_active_special_offers()
    return render_template('special_offers/monthly_specials_list.html',
                           title="Квартиры месяца",
                           offers=active_offers)


@main_bp.route('/special-offer/<int:sell_id>')
@login_required
def special_offer_detail(sell_id):
    """Отображает детальную страницу спец. предложения."""
    offer_details = special_offer_service.get_special_offer_details_by_sell_id(sell_id)
    if not offer_details:
        abort(404)

    # Дополнительно получаем стандартную карточку квартиры для полной информации
    full_card_data = get_apartment_card_data(sell_id)

    return render_template('special_offers/special_offer_detail.html',
                           title=f"Спецпредложение: Квартира {sell_id}",
                           offer=offer_details,
                           card_data=full_card_data)


@main_bp.route('/fix-permissions')
@login_required
def fix_permissions():
    """Разовый маршрут для исправления прав доступа."""
    if not current_user.role or current_user.role.name != 'ADMIN':
        return "Доступ только для администраторов!", 403

    # 1. Находим роль ADMIN
    admin_role = auth_models.Role.query.filter_by(name='ADMIN').first()
    if not admin_role:
        return "Ошибка: роль 'ADMIN' не найдена."

    # 2. Находим (или создаем) право 'manage_specials'
    permission_to_add = auth_models.Permission.query.filter_by(name='manage_specials').first()
    if not permission_to_add:
        permission_to_add = auth_models.Permission(name='manage_specials', description='Управление квартирами месяца')
        db.session.add(permission_to_add)
        # Сразу коммитим, чтобы право появилось в БД
        db.session.commit()

    # 3. Проверяем, есть ли уже это право у роли
    has_permission = any(p.id == permission_to_add.id for p in admin_role.permissions)

    if not has_permission:
        admin_role.permissions.append(permission_to_add)
        db.session.commit()
        return "Успех! Право 'manage_specials' было добавлено к роли ADMIN. Теперь страница должна открыться."
    else:
        return "Право 'manage_specials' уже было у роли ADMIN. Проблема может быть в другом."
