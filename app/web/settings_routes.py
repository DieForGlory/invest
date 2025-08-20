# app/web/settings_routes.py

from flask import Blueprint, render_template, request, flash, redirect, url_for
from flask_login import login_required
from app.core.decorators import permission_required
from app.services import settings_service
from .forms import CalculatorSettingsForm, DealStatusSettingsForm
from ..core.extensions import db
from ..models import auth_models
from ..models.estate_models import EstateHouse
from ..services import data_service
from flask_login import current_user
settings_bp = Blueprint('settings', __name__, template_folder='templates')

@settings_bp.route('/calculator-settings', methods=['GET', 'POST'])
@login_required
@permission_required('manage_settings')
def manage_settings():
    form = CalculatorSettingsForm()
    settings = settings_service.get_calculator_settings()

    if form.validate_on_submit():
        settings_service.update_calculator_settings(request.form)
        flash('Настройки калькуляторов успешно обновлены.', 'success')
        return redirect(url_for('settings.manage_settings'))

    # Заполняем форму текущими значениями из БД
    form.standard_installment_whitelist.data = settings.standard_installment_whitelist
    form.dp_installment_whitelist.data = settings.dp_installment_whitelist
    form.dp_installment_max_term.data = settings.dp_installment_max_term
    form.time_value_rate_annual.data = settings.time_value_rate_annual
    if hasattr(settings, 'standard_installment_min_dp_percent'):
        form.standard_installment_min_dp_percent.data = settings.standard_installment_min_dp_percent

    return render_template('settings/calculator_settings.html', title="Настройки калькуляторов", form=form)


@settings_bp.route('/deal-status-settings', methods=['GET', 'POST'])
@login_required
@permission_required('manage_settings')
def deal_status_settings():
    """Страница для управления статусами сделок и остатков."""
    form = DealStatusSettingsForm()

    # Получаем два РАЗНЫХ списка статусов
    all_deal_statuses = data_service.get_all_deal_statuses()
    all_sell_statuses = data_service.get_all_sell_statuses()

    form.deal_statuses.choices = [(s, s) for s in all_deal_statuses]
    form.inventory_statuses.choices = [(s, s) for s in all_sell_statuses]

    if form.validate_on_submit():
        current_user.company.deal_statuses = ','.join(form.deal_statuses.data)
        current_user.company.inventory_statuses = ','.join(form.inventory_statuses.data)
        db.session.commit()
        flash('Настройки статусов успешно обновлены.', 'success')
        return redirect(url_for('settings.deal_status_settings'))

    if request.method == 'GET':
        form.deal_statuses.data = current_user.company.sale_statuses
        form.inventory_statuses.data = current_user.company.inventory_status_list

    return render_template(
        'settings/deal_status_settings.html',
        title="Настройки статусов",
        form=form
    )

@settings_bp.route('/manage-inventory-exclusions', methods=['GET', 'POST'])
@login_required
@permission_required('manage_settings')
def manage_inventory_exclusions():
    """Страница для управления исключенными ЖК из сводки по остаткам."""
    if request.method == 'POST':
        complex_name = request.form.get('complex_name')
        if complex_name:
            message, category = settings_service.toggle_complex_exclusion(complex_name)
            flash(message, category)
        return redirect(url_for('settings.manage_inventory_exclusions'))

    # Получаем список всех ЖК и исключенных ЖК
    all_complexes = db.session.query(EstateHouse.complex_name).distinct().order_by(EstateHouse.complex_name).all()
    excluded_complexes = settings_service.get_all_excluded_complexes()
    excluded_names = {c.complex_name for c in excluded_complexes}

    return render_template(
        'settings/manage_exclusions.html',
        title="Исключения в сводке по остаткам",
        all_complexes=[c[0] for c in all_complexes],
        excluded_names=excluded_names
    )

@settings_bp.route('/email-recipients', methods=['GET', 'POST'])
@login_required
@permission_required('manage_settings')
def manage_email_recipients():
    """Страница для управления получателями email-уведомлений."""
    if request.method == 'POST':
        selected_user_ids = request.form.getlist('recipient_ids', type=int)

        # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
        # Обращаемся к модели через auth_models
        auth_models.EmailRecipient.query.delete()

        for user_id in selected_user_ids:
            recipient = auth_models.EmailRecipient(user_id=user_id)
            db.session.add(recipient)

        db.session.commit()
        flash('Список получателей уведомлений успешно обновлен.', 'success')
        return redirect(url_for('settings.manage_email_recipients'))

    # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
    # Обращаемся к моделям через auth_models
    all_users = auth_models.User.query.order_by(auth_models.User.full_name).all()
    subscribed_user_ids = {r.user_id for r in auth_models.EmailRecipient.query.all()}

    return render_template(
        'settings/manage_recipients.html',
        title="Получатели уведомлений",
        all_users=all_users,
        subscribed_user_ids=subscribed_user_ids
    )