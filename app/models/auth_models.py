# app/models/auth_models.py

from app.core.extensions import db
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash

# --- НОВАЯ МОДЕЛЬ КОМПАНИИ ---
class Company(db.Model):
    __tablename__ = 'companies'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), unique=True, nullable=False)
    subdomain = db.Column(db.String(120), unique=True, nullable=False, index=True)

    # --- ИЗМЕНЕНИЯ: Два поля для двух баз данных ---
    # Для локальных данных (SQLite)
    db_uri = db.Column(db.String(255), nullable=False)
    # Для фактических данных (MySQL, read-only)
    mysql_db_uri = db.Column(db.String(255), nullable=True)

    # --- Настройки почты ---
    mail_server = db.Column(db.String(120), default='mail.gh.uz')
    mail_port = db.Column(db.Integer, default=587)
    mail_use_tls = db.Column(db.Boolean, default=True)
    mail_username = db.Column(db.String(120), default='robot@gh.uz')
    mail_password = db.Column(db.String(120), nullable=True)

    users = db.relationship('User', back_populates='company')

    def __repr__(self):
        return f'<Company {self.name}>'


class Role(db.Model):
    __tablename__ = 'roles'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80), unique=True, nullable=False)
    users = db.relationship('User', back_populates='role')
    permissions = db.relationship('Permission', secondary='role_permissions', back_populates='roles')

    def __repr__(self):
        return f'<Role {self.name}>'

class Permission(db.Model):
    __tablename__ = 'permissions'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), unique=True, nullable=False)
    description = db.Column(db.String(255))
    roles = db.relationship('Role', secondary='role_permissions', back_populates='permissions')


class User(db.Model, UserMixin):
    __tablename__ = 'system_users'  # Changed to avoid conflict with sales managers table
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), index=True, unique=True, nullable=False)
    full_name = db.Column(db.String(120), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False, index=True)
    phone_number = db.Column(db.String(20), nullable=True)
    password_hash = db.Column(db.String(256))
    role_id = db.Column(db.Integer, db.ForeignKey('roles.id'))
    post_title = db.Column(db.String(255), nullable=True)
    is_active = db.Column(db.Boolean, default=True)
    # --- СВЯЗЬ ПОЛЬЗОВАТЕЛЯ С КОМПАНИЕЙ ---
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'), nullable=False)

    # Add discriminator column for model inheritance
    user_type = db.Column(db.String(50))
    __mapper_args__ = {
        'polymorphic_identity': 'user',
        'polymorphic_on': user_type
    }

    role = db.relationship('Role', back_populates='users')
    company = db.relationship('Company', back_populates='users')

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)
    def can(self, permission_name):
        if self.role:
            return any(p.name == permission_name for p in self.role.permissions)
        return False
    def __repr__(self):
        return f'<User {self.username}>'

class EmailRecipient(db.Model):
    __tablename__ = 'email_recipients'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('system_users.id'), nullable=False, unique=True)
    user = db.relationship('User')

class SalesManager(db.Model):
    __tablename__ = 'users'  # This is the actual sales managers table
    id = db.Column(db.Integer, primary_key=True)
    users_name = db.Column(db.String(255), nullable=False)
    post_title = db.Column(db.String(255), nullable=True)
    is_fired = db.Column(db.Boolean, default=False)
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'), nullable=True)
    
    company = db.relationship('Company')
    
    @property
    def full_name(self):
        return self.users_name
    
    @property
    def is_active(self):
        return not self.is_fired
    
    def __repr__(self):
        return f'<SalesManager {self.full_name}>'

    @property
    def is_fired(self):
        return not self.is_active

role_permissions = db.Table('role_permissions',
    db.Column('role_id', db.Integer, db.ForeignKey('roles.id'), primary_key=True),
    db.Column('permission_id', db.Integer, db.ForeignKey('permissions.id'), primary_key=True)
)