# run.py
import os
from app import create_app
from app.core.config import DevelopmentConfig
from app.core.extensions import db
from app.models import auth_models

# Создаем приложение Flask
app = create_app(DevelopmentConfig)


def setup_database():
    """
    Создает все таблицы, начальные роли, компанию по умолчанию и пользователей 'admin' и 'superadmin'.
    """
    with app.app_context():
        print("\n--- [ОТЛАДКА] Начало функции setup_database ---")

        from app.models import (auth_models, planning_models, estate_models,
                                finance_models, exclusion_models, funnel_models,
                                special_offer_models)

        print("--- [ОТЛАДКА] Вызов db.create_all() для управляющей базы... ---")
        db.create_all()
        print("--- [ОТЛАДКА] db.create_all() завершен. ---")

        if auth_models.Company.query.filter_by(subdomain='default').first() is None:
            print("--- [ОТЛАДКА] Компания 'default' не найдена. Создание... ---")
            tenant_db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'instance', 'tenant_default.db')
            default_company = auth_models.Company(
                name='Default Company',
                subdomain='default',
                db_uri=f'sqlite:///{tenant_db_path}'
            )
            db.session.add(default_company)
            db.session.commit()
            print(f"--- [ОТЛАДКА] Компания 'default' создана. Ее база данных: {default_company.db_uri} ---")

            try:
                engine = db.create_engine(default_company.db_uri)
                models_metadata = [
                    planning_models.db.metadata, estate_models.db.metadata,
                    finance_models.db.metadata, exclusion_models.db.metadata,
                    funnel_models.db.metadata, special_offer_models.db.metadata
                ]
                for metadata in models_metadata:
                    metadata.create_all(bind=engine)
                print(f"--- [ОТЛАДКА] Таблицы в базе 'tenant_default.db' успешно созданы. ---")
            except Exception as e:
                print(f"--- [ОТЛАДКА] ОШИБКА при создании таблиц в базе компании: {e} ---")
        else:
            print("--- [ОТЛАДКА] Компания 'default' уже существует. ---")

        # --- УЛУЧШЕННАЯ ЛОГИКА СОЗДАНИЯ РОЛЕЙ И ПРАВ ---
        print("--- [ОТЛАДКА] Проверка и создание ролей и прав... ---")
        permissions_map = {
            'view_selection': 'Просмотр системы подбора', 'view_discounts': 'Просмотр активной системы скидок',
            'view_version_history': 'Просмотр истории версий скидок',
            'view_plan_fact_report': 'Просмотр План-факт отчета',
            'view_inventory_report': 'Просмотр отчета по остаткам',
            'view_manager_report': 'Просмотр отчетов по менеджерам',
            'view_project_dashboard': 'Просмотр аналитики по проектам',
            'manage_discounts': 'Управление версиями скидок (создание, активация)',
            'manage_settings': 'Управление настройками (калькуляторы, курс)',
            'manage_users': 'Управление пользователями',
            'upload_data': 'Загрузка данных (планы и т.д.)',
            'download_kpi_report': 'Выгрузка ведомости по KPI менеджеров',
            'manage_specials': 'Управление специальными предложениями (акции)'
        }

        all_permissions = {}
        for name, desc in permissions_map.items():
            p = auth_models.Permission.query.filter_by(name=name).first()
            if not p:
                p = auth_models.Permission(name=name, description=desc)
                db.session.add(p)
            all_permissions[name] = p
        db.session.commit()

        roles_permissions = {
            'MPP': ['view_selection', 'view_discounts'],
            'MANAGER': ['view_selection', 'view_discounts', 'view_version_history', 'manage_settings',
                        'view_plan_fact_report', 'view_inventory_report', 'view_manager_report',
                        'view_project_dashboard'],
            'ADMIN': list(permissions_map.keys()),
            'SUPERADMIN': list(permissions_map.keys())
        }

        for role_name, permissions_list in roles_permissions.items():
            role = auth_models.Role.query.filter_by(name=role_name).first()
            if not role:
                role = auth_models.Role(name=role_name)
                db.session.add(role)
                db.session.flush()  # Получаем ID для связи
                for p_name in permissions_list:
                    if p_name in all_permissions:
                        role.permissions.append(all_permissions[p_name])
        db.session.commit()
        print("--- [ОТЛАДКА] Роли и права успешно созданы/проверены. ---")

        if auth_models.User.query.filter_by(username='admin').first() is None:
            print("--- [ОТЛАДКА] Пользователь 'admin' не найден. Создание... ---")
            admin_role = auth_models.Role.query.filter_by(name='ADMIN').first()
            default_company = auth_models.Company.query.filter_by(subdomain='default').first()
            if admin_role and default_company:
                admin_user = auth_models.User(
                    username='admin',
                    role=admin_role,
                    full_name='Администратор Системы',
                    email='d.plakhotnyi@gh.uz',
                    company_id=default_company.id
                )
                admin_user.set_password('admin')
                db.session.add(admin_user)
                db.session.commit()
                print("--- [ОТЛАДКА] Пользователь 'admin' успешно создан. ---")
            else:
                print("--- [ОТЛАДКА] КРИТИЧЕСКАЯ ОШИБКА: Роль ADMIN или компания 'default' не найдены! ---")
        else:
            print("--- [ОТЛАДКА] Пользователь 'admin' уже существует. ---")

        print("--- [ОТЛАДКА] Проверка существования пользователя 'superadmin'...")
        if auth_models.User.query.filter_by(username='superadmin').first() is None:
            print("--- [ОТЛАДКА] Пользователь 'superadmin' не найден. Создание... ---")
            superadmin_role = auth_models.Role.query.filter_by(name='SUPERADMIN').first()
            default_company = auth_models.Company.query.filter_by(subdomain='default').first()

            if superadmin_role and default_company:
                superadmin_user = auth_models.User(
                    username='superadmin',
                    role=superadmin_role,
                    full_name='Главный Администратор',
                    email='superadmin@example.com',
                    company_id=default_company.id
                )
                superadmin_user.set_password('superadmin')
                db.session.add(superadmin_user)
                db.session.commit()
                print("--- [ОТЛАДКА] Пользователь 'superadmin' успешно создан. ---")
            else:
                print("--- [ОТЛАДКА] КРИТИЧЕСКАЯ ОШИБКА: Роль SUPERADMIN или компания 'default' не найдены! ---")
        else:
            print("--- [ОТЛАДКА] Пользователь 'superadmin' уже существует. ---")

        print("--- [ОТЛАДКА] Функция setup_database завершена. ---\n")


if os.environ.get('WERKZEUG_RUN_MAIN') is None:
    setup_database()

if __name__ == '__main__':
    print("[FLASK APP] 🚦 Запуск веб-сервера Flask...")
    app.run(host='0.0.0.0', port=5001, debug=True)