# app/core/config.py

import os

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'a-very-secret-key'
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Глобальные настройки почты удалены, т.к. они теперь для каждой компании свои
    USD_TO_UZS_RATE = 13050.0

class DevelopmentConfig(Config):
    DEBUG = True
    # Указываем путь к нашей новой "управляющей" базе данных.
    # Здесь будут храниться пользователи и информация о компаниях-клиентах.
    SQLALCHEMY_DATABASE_URI = os.environ.get('CONTROL_DATABASE_URL') or 'sqlite:///control_app.db'