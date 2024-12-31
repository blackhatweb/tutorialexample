from abc import ABC, abstractmethod
from sqlalchemy import and_, or_
from sqlalchemy.orm import Session
from typing import List, Any, Dict

class FilterCriteria(ABC):
    @abstractmethod
    def apply(self, query: Any) -> Any:
        pass

class SimpleFilter(FilterCriteria):
    def __init__(self, field: str, value: Any, operator: str = '=='):
        self.field = field
        self.value = value
        self.operator = operator

    def apply(self, query):
        if self.operator == '==':
            return query.filter(getattr(query.column_descriptions[0]['type'], self.field) == self.value)
        elif self.operator == 'like':
            return query.filter(getattr(query.column_descriptions[0]['type'], self.field).like(f'%{self.value}%'))
        elif self.operator == '>':
            return query.filter(getattr(query.column_descriptions[0]['type'], self.field) > self.value)
        elif self.operator == '<':
            return query.filter(getattr(query.column_descriptions[0]['type'], self.field) < self.value)

class CompositeFilter(FilterCriteria):
    def __init__(self, filters: List[FilterCriteria], operator: str = 'and'):
        self.filters = filters
        self.operator = operator

    def apply(self, query):
        conditions = [f.apply(query) for f in self.filters]
        if self.operator == 'and':
            return query.filter(and_(*conditions))
        return query.filter(or_(*conditions))

class TableFilterUseCase:
    def __init__(self, session: Session, model: Any):
        self.session = session
        self.model = model
        
    def execute(self, filter_criteria: FilterCriteria = None):
        query = self.session.query(self.model)
        if filter_criteria:
            query = filter_criteria.apply(query)
        return query.all()

# Example usage:
"""
# Model definition
class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)

# Creating filters
name_filter = SimpleFilter('name', 'John', 'like')
age_filter = SimpleFilter('age', 18, '>')
composite = CompositeFilter([name_filter, age_filter], 'and')

# Using the use case
filter_use_case = TableFilterUseCase(session, User)
results = filter_use_case.execute(composite)
"""

from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import and_, or_
from typing import List, Optional, Tuple, Union

# Định nghĩa model (ví dụ)
metadata = MetaData()
users_table = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String),
    Column("age", Integer),
    Column("city", String),
)

class FilterCondition:
    def __init__(self, column: Column, operator: str, value: Union[str, int]):
        self.column = column
        self.operator = operator
        self.value = value

    def to_expression(self):
        if self.operator == "=":
            return self.column == self.value
        elif self.operator == "!=":
            return self.column != self.value
        elif self.operator == ">":
            return self.column > self.value
        elif self.operator == "<":
            return self.column < self.value
        elif self.operator == ">=":
            return self.column >= self.value
        elif self.operator == "<=":
            return self.column <= self.value
        elif self.operator == "like":
            return self.column.like(f"%{self.value}%") #LIKE '%value%'
        elif self.operator == "ilike":
            return self.column.ilike(f"%{self.value}%") #ILIKE '%value%' (case-insensitive)
        else:
            raise ValueError(f"Invalid operator: {self.operator}")

class TableFilter:
    def __init__(self, table: Table, session):
        self.table = table
        self.session = session
        self.conditions: List[FilterCondition] = []

    def add_condition(self, column: Column, operator: str, value: Union[str, int]):
        self.conditions.append(FilterCondition(column, operator, value))
        return self # cho phép chaining methods

    def apply(self, use_or:bool = False):
        if not self.conditions:
            return self.session.query(self.table).all()

        expressions = [condition.to_expression() for condition in self.conditions]

        if use_or:
            combined_expression = or_(*expressions)
        else:
            combined_expression = and_(*expressions)

        return self.session.query(self.table).filter(combined_expression).all()

# Ví dụ sử dụng
engine = create_engine("sqlite:///:memory:") # Sử dụng SQLite in-memory cho ví dụ
metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# Tạo dữ liệu mẫu
from sqlalchemy import insert
insert_stmt = insert(users_table).values([
    {"name": "Alice", "age": 30, "city": "New York"},
    {"name": "Bob", "age": 25, "city": "Los Angeles"},
    {"name": "Charlie", "age": 35, "city": "Chicago"},
    {"name": "David", "age": 28, "city": "New York"},
])
session.execute(insert_stmt)
session.commit()

# Sử dụng bộ lọc
table_filter = TableFilter(users_table, session)
results = (
    table_filter.add_condition(users_table.c.age, ">", 27)
                .add_condition(users_table.c.city, "=", "New York")
                .apply()
)

for row in results:
    print(row)

table_filter = TableFilter(users_table, session)
results_or = (
    table_filter.add_condition(users_table.c.city, "=", "New York")
                .add_condition(users_table.c.city, "=", "Los Angeles")
                .apply(use_or=True)
)

for row in results_or:
    print(row)

session.close()