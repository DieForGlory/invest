# app/__init__.py
import os
import json
from datetime import date, datetime
from flask import Flask, request, render_template, g, session, current_app, abort
from flask_login import LoginManager, current_user
from flask_cors import CORS
from flask_migrate import Migrate
from flask_babel import Babel
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine  # <-- ДОБАВЛЕН ИМПОРТ
from decimal import Decimal
from .core.config import DevelopmentConfig
from .core.extensions import db

# 1. Инициализация расширений
login_manager = LoginManager()
login_manager.login_view = 'auth.login'
login_manager.login_message = "Пожалуйста, войдите в систему для доступа к этой странице."
login_manager.login_message_category = "info"
babel = Babel()


# 2. Пользовательский кодировщик для JSON
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, (date, datetime)):
                return obj.isoformat()
            # --- НАЧАЛО ИЗМЕНЕНИЯ ---
            if isinstance(obj, Decimal):
                return float(obj)  # Преобразуем Decimal в float
            # --- КОНЕЦ ИЗМЕНЕНИЯ ---
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return json.JSONEncoder.default(self, obj)


# 3. Функция для выбора языка
def select_locale():
    if 'language' in session and session['language'] in current_app.config['LANGUAGES'].keys():
        return session['language']
    return request.accept_languages.best_match(current_app.config['LANGUAGES'].keys())


def create_app(config_class=DevelopmentConfig):
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(config_class)
    app.config['BABEL_DEFAULT_LOCALE'] = 'ru'
    app.config['LANGUAGES'] = {'en': 'English', 'ru': 'Русский'}

    CORS(app)
    db.init_app(app)
    Migrate(app, db)
    login_manager.init_app(app)
    babel.init_app(app, locale_selector=select_locale)
    app.json_encoder = CustomJSONEncoder

    try:
        os.makedirs(app.instance_path, exist_ok=True)
    except OSError as e:
        print(f"Ошибка при создании папки instance: {e}")

    with app.app_context():
        # Импорт моделей
        from .models import auth_models

        # Регистрация Blueprints
        from .web.main_routes import main_bp
        from .web.auth_routes import auth_bp
        from .web.discount_routes import discount_bp
        from .web.report_routes import report_bp
        from .web.complex_calc_routes import complex_calc_bp
        from .web.settings_routes import settings_bp
        from .web.api_routes import api_bp
        from .web.special_offer_routes import special_offer_bp
        from .web.manager_analytics_routes import manager_analytics_bp
        from .web.super_admin_routes import super_admin_bp

        app.register_blueprint(report_bp, url_prefix='/reports')
        app.register_blueprint(main_bp)
        app.register_blueprint(auth_bp)
        app.register_blueprint(discount_bp)
        app.register_blueprint(complex_calc_bp)
        app.register_blueprint(settings_bp)
        app.register_blueprint(api_bp, url_prefix='/api/v1')
        app.register_blueprint(special_offer_bp, url_prefix='/specials')
        app.register_blueprint(manager_analytics_bp, url_prefix='/manager-analytics')
        app.register_blueprint(super_admin_bp)

        @login_manager.user_loader
        def load_user(user_id):
            return auth_models.User.query.get(int(user_id))

    @app.before_request
    def before_request_tasks():
        g.lang = str(select_locale())

        if request.endpoint and (
                'static' in request.endpoint or 'auth.' in request.endpoint or 'super_admin.' in request.endpoint):
            return

        if not current_user.is_authenticated:
            return

        company = current_user.company
        if not company:
            return abort(403, "Пользователь не привязан к компании.")

        try:
            local_engine = create_engine(company.db_uri)
            g.company_db_session = sessionmaker(bind=local_engine)()
        except Exception as e:
            print(f"CRITICAL: Could not connect to tenant LOCAL DB for {company.name}. Error: {e}")
            return abort(500, "Не удалось подключиться к локальной базе данных компании.")

        if company.mysql_db_uri:
            try:
                # --- ГЛАВНОЕ ИЗМЕНЕНИЕ: Добавляем опции echo и isolation_level ---
                mysql_engine = create_engine(
                    company.mysql_db_uri,
                    echo=True,  # Включаем полное логирование SQL
                    connect_args={"init_command": "SET NAMES utf8mb4"},
                    isolation_level="READ COMMITTED"  # Заставляем читать актуальные данные
                )
                g.mysql_db_session = sessionmaker(bind=mysql_engine)()
                print("--- [DEBUG] ✔️ Подключение к MYSQL успешно c новыми параметрами! ---")
            except Exception as e:
                print(f"CRITICAL: Could not connect to tenant MYSQL DB for {company.name}. Error: {e}")
                return abort(500, "Не удалось подключиться к внешней базе данных MySQL.")
        else:
            g.mysql_db_session = None

    @app.teardown_request
    def teardown_request(exception=None):
        if hasattr(g, 'company_db_session'):
            g.company_db_session.close()
        if hasattr(g, 'mysql_db_session') and g.mysql_db_session:
            g.mysql_db_session.close()

    return app