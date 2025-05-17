from models.base import app, db
from models.sensor import Sensor
app.app_context().push()
db.create_all()
