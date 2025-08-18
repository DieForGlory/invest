import os
from flask import Blueprint, render_template, request, flash, redirect, url_for, current_app
from flask_login import login_required, current_user
from sqlalchemy import create_engine

from ..core.extensions import db
from ..models import auth_models, planning_models, estate_models, finance_models, exclusion_models, funnel_models, \
    special_offer_models
from .forms import CreateCompanyForm, CreateUserForm

super_admin_bp = Blueprint('super_admin', __name__, template_folder='templates')


def is_super_admin():
    return current_user.is_authenticated and current_user.role and current_user.role.name == 'SUPERADMIN'


@super_admin_bp.before_request
def before_request_hook():
    if not is_super_admin():
        flash('У вас нет прав для доступа к этой странице.', 'danger')
        return redirect(url_for('main.index'))


@super_admin_bp.route('/super-admin', methods=['GET', 'POST'])
def dashboard():
    form = CreateCompanyForm()
    if form.validate_on_submit():
        try:
            # --- ИЗМЕНЕНИЕ: Формируем ОБЕ строки подключения ---
            # 1. Строка для MySQL (read-only)
            mysql_db_uri = (f"mysql+pymysql://{form.db_user.data}:{form.db_password.data}@"
                            f"{form.db_host.data}/{form.db_name.data}")

            # 2. Строка для локальной SQLite (read-write)
            subdomain = form.subdomain.data
            local_db_filename = f"tenant_{subdomain}.db"
            local_db_path = os.path.join(current_app.instance_path, local_db_filename)
            local_db_uri = f"sqlite:///{local_db_path}"

            # Создаем компанию с двумя путями к БД
            new_company = auth_models.Company(
                name=form.name.data,
                subdomain=subdomain,
                db_uri=local_db_uri,
                mysql_db_uri=mysql_db_uri,
                mail_server=form.mail_server.data,
                mail_port=form.mail_port.data,
                mail_use_tls=form.mail_use_tls.data,
                mail_username=form.mail_username.data,
                mail_password=form.mail_password.data
            )
            db.session.add(new_company)
            db.session.commit()
            flash(f"Компания '{new_company.name}' успешно создана.", "success")

            # 3. Создаем таблицы только в локальной SQLite базе
            engine = create_engine(local_db_uri)
            models_metadata = [
                planning_models.db.metadata,
                exclusion_models.db.metadata,
                special_offer_models.db.metadata,
                finance_models.db.metadata  # Для CurrencySettings
            ]
            for metadata in models_metadata:
                metadata.create_all(bind=engine)

            flash(f"Локальная база данных для '{new_company.name}' успешно создана.", "info")
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
    del form.role

    if form.validate_on_submit():
        admin_role = auth_models.Role.query.filter_by(name='ADMIN').first()
        if not admin_role:
            flash("Критическая ошибка: роль 'ADMIN' не найдена.", 'danger')
            return redirect(url_for('super_admin.dashboard'))

        new_admin = auth_models.User(
            username=form.username.data,
            full_name=form.full_name.data,
            email=form.email.data,
            phone_number=form.phone_number.data,
            role_id=admin_role.id,
            company_id=company.id
        )
        new_admin.set_password(form.password.data)
        db.session.add(new_admin)
        db.session.commit()
        flash(f"Администратор '{new_admin.username}' для компании '{company.name}' успешно создан.", "success")
        return redirect(url_for('super_admin.dashboard'))

    return render_template('super_admin/create_company_admin.html', title=f"Создать админа для {company.name}", company=company, form=form)