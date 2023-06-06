# models.py

from sqlalchemy import Column, Integer, String, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class CsvData(Base):
    __tablename__ = "csv_data"

    id = Column(Integer, primary_key=True)
    column1 = Column(String)
    column2 = Column(Integer)
    column3 = Column(DateTime)
    column4 = Column(Float)
    # ... define all columns according to your csv schema
