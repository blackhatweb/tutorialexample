from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import and_, or_
from typing import List, Optional, Tuple, Union

# Định nghĩa model (ví dụ)
metadata = MetaData()

users_table = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String),
    Column("city_id", Integer, ForeignKey("cities.id")), # Foreign key
)

cities_table = Table(
    "cities",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String),
)

class FilterCondition:
    # ... (giữ nguyên như trước)
    ...

class TableFilter:
    def __init__(self, session): # Không cần table ở đây nữa
        self.session = session
        self.conditions: List[FilterCondition] = []
        self.joins: List[Tuple[Table, Optional[any]]] = [] # Danh sách các bảng cần join

    def add_condition(self, column: Column, operator: str, value: Union[str, int]):
        self.conditions.append(FilterCondition(column, operator, value))
        return self

    def add_join(self, table: Table, join_condition = None):
        self.joins.append((table, join_condition))
        return self

    def apply(self, use_or: bool = False):
        query = self.session.query(users_table) # Bắt đầu query từ bảng chính

        # Thêm join vào query
        for table, join_condition in self.joins:
            if join_condition:
                query = query.join(table, join_condition)
            else:
                query = query.join(table) # Inner join mặc định

        if self.conditions:
            expressions = [condition.to_expression() for condition in self.conditions]
            if use_or:
                combined_expression = or_(*expressions)
            else:
                combined_expression = and_(*expressions)
            query = query.filter(combined_expression)

        return query.all()

# Ví dụ sử dụng
engine = create_engine("sqlite:///:memory:")
metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# Tạo dữ liệu mẫu
from sqlalchemy import insert

city_insert = insert(cities_table).values([
    {"name": "New York"},
    {"name": "Los Angeles"},
])
session.execute(city_insert)
session.commit()

user_insert = insert(users_table).values([
    {"name": "Alice", "city_id": 1},
    {"name": "Bob", "city_id": 2},
    {"name": "Charlie", "city_id": 1},
])
session.execute(user_insert)
session.commit()

# Sử dụng bộ lọc với join
table_filter = TableFilter(session)
results = (
    table_filter.add_join(cities_table, users_table.c.city_id == cities_table.c.id) # Join bảng cities
                .add_condition(cities_table.c.name, "=", "New York") # Lọc theo tên thành phố
                .apply()
)

for row in results:
    print(row)

# Ví dụ join ngầm định (nếu đã định nghĩa relationship trong ORM)
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    city_id = Column(Integer, ForeignKey("cities.id"))
    city = relationship("City") # Định nghĩa relationship

class City(Base):
    __tablename__ = "cities"
    id = Column(Integer, primary_key=True)
    name = Column(String)

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

table_filter = TableFilter(session)
results_implicit_join = (
    table_filter.add_join(City) # Join ngầm định thông qua relationship
                .add_condition(City.name, "=", "New York")
                .apply()
)
for row in results_implicit_join:
    print(row)

session.close()