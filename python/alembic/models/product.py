from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from . import Base # Import Base
import datetime


class Product(Base):
    __tablename__ = 'products'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    price = Column(Float)
    user_id = Column(Integer, ForeignKey('users.id')) # Foreign key đến bảng users
    user = relationship("User", back_populates='products') # relationship cho phép truy cập dữ liệu ngược lại
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)


    def __repr__(self):
        return f"<Product(name='{self.name}')>"