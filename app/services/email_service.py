# app/services/email_service.py

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from flask import current_app, g
from flask_login import current_user

from ..models import auth_models
from ..core.extensions import db


def send_email(subject, html_body):
    """Отправляет email-сообщение получателям ТЕКУЩЕЙ компании."""

    if not current_user.is_authenticated or not current_user.company:
        print("[EMAIL SERVICE] ❌ ОШИБКА: Не удалось определить компанию для отправки письма.")
        return

    company_config = current_user.company
    sender_email = company_config.mail_username

    # --- ИСПРАВЛЕННАЯ ЛОГИКА ПОЛУЧЕНИЯ АДРЕСАТОВ ---
    # 1. Получаем ID текущей компании
    current_company_id = current_user.company_id

    # 2. Запрашиваем email-ы только тех пользователей, которые:
    #    а) являются получателями (есть в EmailRecipient)
    #    б) принадлежат ТЕКУЩЕЙ компании
    recipients_from_db = g.company_db_session.query(auth_models.User.email).join(
        auth_models.EmailRecipient, auth_models.User.id == auth_models.EmailRecipient.user_id
    ).filter(
        auth_models.User.company_id == current_company_id
    ).all()

    recipients = [email for email, in recipients_from_db]

    # --- Логирование ---
    print("\n" + "=" * 50)
    print(f"[EMAIL SERVICE] 📨 НАЧАЛО ПРОЦЕССА ОТПРАВКИ ПИСЬМА ДЛЯ КОМПАНИИ: {company_config.name}")
    print(f"[EMAIL SERVICE] Отправитель: {sender_email}")
    print(f"[EMAIL SERVICE] Получатели: {recipients}")
    print(f"[EMAIL SERVICE] Тема: {subject}")

    if not recipients:
        print("[EMAIL SERVICE] ❕ ВНИМАНИЕ: Список получателей для этой компании пуст. Отправка отменена.")
        print("=" * 50 + "\n")
        return

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ", ".join(recipients)

    part = MIMEText(html_body, 'html')
    msg.attach(part)

    try:
        print(f"[EMAIL SERVICE] Попытка подключения к серверу: {company_config.mail_server}:{company_config.mail_port}")
        server = smtplib.SMTP(company_config.mail_server, company_config.mail_port)
        server.set_debuglevel(1)

        if company_config.mail_use_tls:
            server.starttls()

        server.login(company_config.mail_username, company_config.mail_password)
        server.sendmail(sender_email, recipients, msg.as_string())

    except Exception as e:
        print(f"[EMAIL SERVICE] ❌ КРИТИЧЕСКАЯ ОШИБКА ПРИ ОТПРАВКЕ: {type(e).__name__}: {e}")
    finally:
        if 'server' in locals() and server:
            server.quit()
        print("[EMAIL SERVICE] 🏁 ЗАВЕРШЕНИЕ ПРОЦЕССА ОТПРАВКИ")
        print("=" * 50 + "\n")