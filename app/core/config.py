# app/core/config.py

import os

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'a-very-secret-key'
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Настройки для отправки Email
    MAIL_SERVER = os.environ.get('MAIL_SERVER', 'mail.gh.uz')
    MAIL_PORT = int(os.environ.get('MAIL_SERVER_PORT', 587))
    MAIL_USE_TLS = True
    MAIL_USERNAME = os.environ.get('SEND_FROM_EMAIL', 'robot@gh.uz')
    MAIL_PASSWORD = os.environ.get('SEND_FROM_EMAIL_PASSWORD', 'ABwHRMp1')
    MAIL_RECIPIENTS = ['d.plakhotnyi@gh.uz']
    USD_TO_UZS_RATE = 13050.0

class DevelopmentConfig(Config):
    DEBUG = True
    # Указываем путь к нашей новой "управляющей" базе данных.
    # Здесь будут храниться пользователи и информация о компаниях-клиентах.
    SQLALCHEMY_DATABASE_URI = os.environ.get('CONTROL_DATABASE_URL') or 'sqlite:///control_app.db'