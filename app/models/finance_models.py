# app/models/finance_models.py
from app.core.extensions import db


class FinanceOperation(db.Model):
    __tablename__ = 'finances'

    id = db.Column(db.Integer, primary_key=True)
    estate_sell_id = db.Column(db.Integer, db.ForeignKey('estate_sells.id'), nullable=False)
    summa = db.Column(db.Float)
    status_name = db.Column(db.String(100))
    payment_type = db.Column(db.String(100), name='types_name')
    date_added = db.Column(db.Date)
    date_to = db.Column(db.Date, nullable=True)
    manager_id = db.Column(db.Integer, name='respons_manager_id')
    # data_hash = db.Column(db.String(64), index=True, nullable=True) # <-- СТРОКА УДАЛЕНА
    sell = db.relationship('EstateSell')

class CurrencySettings(db.Model):
    __tablename__ = 'currency_settings'
    id = db.Column(db.Integer, primary_key=True)
    rate_source = db.Column(db.String(10), default='cbu', nullable=False)
    cbu_rate = db.Column(db.Float, default=0.0)
    manual_rate = db.Column(db.Float, default=0.0)
    effective_rate = db.Column(db.Float, default=0.0)
    cbu_last_updated = db.Column(db.DateTime)

    def update_effective_rate(self):
        if self.rate_source == 'cbu':
            self.effective_rate = self.cbu_rate
        else:
            self.effective_rate = self.manual_rate