# app/__init__.py
import os
import json
from datetime import date, datetime
from flask import Flask, request, render_template, g, session, current_app
from flask_login import LoginManager
from flask_apscheduler import APScheduler
from flask_cors import CORS
from flask_migrate import Migrate
from flask_babel import Babel

from .core.config import DevelopmentConfig
from .core.extensions import db

# 1. Инициализация расширений
login_manager = LoginManager()
login_manager.login_view = 'auth.login'
login_manager.login_message = "Пожалуйста, войдите в систему для доступа к этой странице."
login_manager.login_message_category = "info"
babel = Babel()
scheduler = APScheduler()

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

# 3. Функция для выбора языка (определяется до create_app)
def select_locale():
    # Пытаемся получить язык из сессии
    if 'language' in session and session['language'] in current_app.config['LANGUAGES'].keys():
        return session['language']
    # Если нет, используем лучший вариант на основе заголовков запроса
    return request.accept_languages.best_match(current_app.config['LANGUAGES'].keys())


def create_app(config_class=DevelopmentConfig):
    """
    Фабрика для создания и конфигурации экземпляра приложения Flask.
    """
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(config_class)

    # Конфигурация для мультиязычности
    app.config['BABEL_DEFAULT_LOCALE'] = 'ru'
    app.config['LANGUAGES'] = {'en': 'English', 'ru': 'Русский'}

    # Инициализация всех расширений
    CORS(app)
    db.init_app(app)
    Migrate(app, db)
    login_manager.init_app(app)
    babel.init_app(app, locale_selector=select_locale)
    scheduler.init_app(app)
    app.json_encoder = CustomJSONEncoder

    # Создание директории instance, если ее нет
    try:
        os.makedirs(app.instance_path, exist_ok=True)
    except OSError as e:
        print(f"Ошибка при создании папки instance: {e}")

    # Запуск планировщика задач (только в основном процессе)
    if not app.debug or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        scheduler.start()

    # Контекст приложения для регистрации Blueprints и других операций
    with app.app_context():
        # Импорт моделей
        from .models import auth_models, planning_models, estate_models, finance_models, exclusion_models, funnel_models, special_offer_models

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

        # Загрузчик пользователя для Flask-Login
        @login_manager.user_loader
        def load_user(user_id):
            return auth_models.User.query.get(int(user_id))

        # Добавление задачи в планировщик
        if scheduler.running and not scheduler.get_job('update_cbu_rate_job'):
            scheduler.add_job(
                id='update_cbu_rate_job',
                func='app.services.currency_service:fetch_and_update_cbu_rate',
                trigger='interval',
                hours=1
            )

    # Единая функция, выполняемая перед каждым запросом
    @app.before_request
    def before_request_tasks():
        # Установка языка для шаблонов
        g.lang = str(select_locale())

        # Проверка на файл блокировки обновления
        lock_file_path = os.path.join(app.instance_path, 'update.lock')
        if os.path.exists(lock_file_path) and request.endpoint != 'static':
            return render_template('standolone/update_in_progress.html')

    return app