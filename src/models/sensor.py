from models.base import db

class Sensor(db.Model):
    __tablename__ = 'Sensor'
    id = db.Column(db.String(10), primary_key=True)
    type = db.Column(db.String(10), nullable=False)
    feeder = db.Column(db.String(10), nullable=False)
    segment = db.Column(db.String(10), nullable=True)
    latitude = db.Column(db.Float, nullable=True)
    longitude = db.Column(db.Float, nullable=True)
        
    def __repr__(self):
        return f'<Sensor {self.id}>'
