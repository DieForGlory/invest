# app/web/super_admin_routes.py

from flask import Blueprint, render_template, request, flash, redirect, url_for
from flask_login import login_required, current_user
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ..core.decorators import permission_required
from ..core.extensions import db
from ..models import auth_models, planning_models, estate_models, finance_models, exclusion_models, funnel_models, special_offer_models
from .forms import CreateCompanyForm, CreateUserForm  # Мы немного адаптируем CreateUserForm

super_admin_bp = Blueprint('super_admin', __name__, template_folder='templates')

def is_super_admin():
    """Проверяет, имеет ли текущий пользователь роль SUPER_ADMIN."""
    # Эта проверка безопасности - ключевая для защиты панели.
    return current_user.is_authenticated and current_user.role and current_user.role.name == 'SUPER_ADMIN'

@super_admin_bp.before_request
def before_request_hook():
    """Защита всех маршрутов в этом blueprint."""
    if not is_super_admin():
        flash('У вас нет прав для доступа к этой странице.', 'danger')
        return redirect(url_for('main.index'))

@super_admin_bp.route('/super-admin', methods=['GET', 'POST'])
def dashboard():
    """Главная страница суперадминки для создания и просмотра компаний."""
    form = CreateCompanyForm()
    if form.validate_on_submit():
        try:
            # 1. Формируем строку подключения к MySQL
            db_uri = (f"mysql+pymysql://{form.db_user.data}:{form.db_password.data}@"
                      f"{form.db_host.data}/{form.db_name.data}")

            # 2. Создаем новую компанию
            new_company = auth_models.Company(
                name=form.name.data,
                subdomain=form.subdomain.data,
                db_uri=db_uri
            )
            db.session.add(new_company)
            db.session.commit()
            flash(f"Компания '{new_company.name}' успешно создана.", "success")

            # 3. Создаем все необходимые таблицы в новой базе данных
            engine = create_engine(db_uri)
            # Собираем метаданные всех моделей, которые должны быть в базе клиента
            models_metadata = [
                planning_models.db.metadata, estate_models.db.metadata,
                finance_models.db.metadata, exclusion_models.db.metadata,
                funnel_models.db.metadata, special_offer_models.db.metadata
            ]
            for metadata in models_metadata:
                metadata.create_all(bind=engine)

            flash(f"Таблицы в базе данных для '{new_company.name}' успешно созданы.", "info")
            return redirect(url_for('super_admin.dashboard'))

        except Exception as e:
            db.session.rollback()
            flash(f"Произошла ошибка при создании компании или ее БД: {e}", "danger")

    companies = auth_models.Company.query.order_by(auth_models.Company.name).all()
    return render_template('super_admin/dashboard.html', title="Super Admin Dashboard", companies=companies, form=form)


@super_admin_bp.route('/super-admin/company/<int:company_id>/create-admin', methods=['GET', 'POST'])
def create_company_admin(company_id):
    """Страница для создания администратора для конкретной компании."""
    company = auth_models.Company.query.get_or_404(company_id)
    form = CreateUserForm()
    # Удаляем выбор роли, т.к. мы всегда создаем ADMIN'а
    del form.role

    if form.validate_on_submit():
        # Находим роль ADMIN в главной (управляющей) базе данных
        admin_role = auth_models.Role.query.filter_by(name='ADMIN').first()
        if not admin_role:
            flash("Критическая ошибка: роль 'ADMIN' не найдена.", 'danger')
            return redirect(url_for('super_admin.dashboard'))

        # Создаем пользователя
        new_admin = auth_models.User(
            username=form.username.data,
            full_name=form.full_name.data,
            email=form.email.data,
            phone_number=form.phone_number.data,
            role_id=admin_role.id,
            company_id=company.id  # Привязываем к компании
        )
        new_admin.set_password(form.password.data)
        db.session.add(new_admin)
        db.session.commit()
        flash(f"Администратор '{new_admin.username}' для компании '{company.name}' успешно создан.", "success")
        return redirect(url_for('super_admin.dashboard'))

    return render_template('super_admin/create_company_admin.html', title=f"Создать админа для {company.name}", company=company, form=form)