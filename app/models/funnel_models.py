# app/models/funnel_models.py
from app.core.extensions import db


class EstateBuy(db.Model):
    __tablename__ = 'estate_buys'

    id = db.Column(db.Integer, primary_key=True)
    date_added = db.Column(db.Date)
    created_at = db.Column(db.DateTime)
    status_name = db.Column(db.String(32))
    custom_status_name = db.Column(db.String(255))

class EstateBuysStatusLog(db.Model):
    __tablename__ = 'estate_buys_statuses_log'
    manager_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    manager = db.relationship('SalesManager')
    id = db.Column(db.Integer, primary_key=True)
    log_date = db.Column(db.DateTime)
    estate_buy_id = db.Column(db.Integer)
    status_to_name = db.Column(db.String(32))
    status_custom_to_name = db.Column(db.String(255))