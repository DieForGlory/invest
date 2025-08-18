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
        # Импорт моделей (теперь только auth_models здесь для user_loader)
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

        app.register_blueprint(report_bp, url_prefix='/reports')
        app.register_blueprint(main_bp)
        app.register_blueprint(auth_bp)
        app.register_blueprint(discount_bp)
        app.register_blueprint(complex_calc_bp)
        app.register_blueprint(settings_bp)
        app.register_blueprint(api_bp, url_prefix='/api/v1')
        app.register_blueprint(special_offer_bp, url_prefix='/specials')
        app.register_blueprint(manager_analytics_bp, url_prefix='/manager-analytics')

        @login_manager.user_loader
        def load_user(user_id):
            return auth_models.User.query.get(int(user_id))

    # --- НАЧАЛО: КЛЮЧЕВАЯ ЛОГИКА МУЛЬТИ-АРЕНДНОСТИ ---
    @app.before_request
    def before_request_tasks():
        g.lang = str(select_locale())

        # Для страниц логина и статики подключение к базе компании не нужно
        if request.endpoint and ('static' in request.endpoint or 'auth.' in request.endpoint):
            return

        # Если пользователь не аутентифицирован, ничего не делаем
        if not current_user.is_authenticated:
            return

        # Получаем компанию текущего пользователя
        company = current_user.company
        if not company:
            # Этого не должно случиться, если все пользователи привязаны к компаниям
            return abort(403, "Пользователь не привязан к компании.")

        # Создаем движок и сессию для базы данных этой компании
        try:
            engine = db.create_engine(company.db_uri, {})
            # Сохраняем сессию в глобальном объекте `g` на время запроса
            g.company_db_session = sessionmaker(bind=engine)()
        except Exception as e:
            # Обработка ошибки, если не удалось подключиться к базе компании
            print(f"CRITICAL: Could not connect to tenant DB for {company.name}. Error: {e}")
            return abort(500, "Не удалось подключиться к базе данных компании.")

    @app.teardown_request
    def teardown_request(exception=None):
        # Гарантированно закрываем сессию после каждого запроса, чтобы избежать утечек
        if hasattr(g, 'company_db_session'):
            g.company_db_session.close()
    # --- КОНЕЦ: КЛЮЧЕВАЯ ЛОГИКА МУЛЬТИ-АРЕНДНОСТИ ---

    return app