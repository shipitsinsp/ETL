import os

SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', 'thisISaSECRET_1234')
SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'