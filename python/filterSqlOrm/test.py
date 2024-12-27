from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from typing import List, Any

# Định nghĩa các model
Base = declarative_base()

class Product(Base):
    __tablename__ = 'products'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    category_id = Column(Integer, ForeignKey('categories.id'))
    category = relationship("Category", back_populates="products")
    price = Column(Integer)

class Category(Base):
    __tablename__ = 'categories'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    products = relationship("Product", back_populates="category")

# Interface cho Specification
class Specification:
    def to_expression(self, model):
        raise NotImplementedError

# Các Concrete Specifications
class EqualsSpecification(Specification):
    def __init__(self, attribute: str, value: Any):
        self.attribute = attribute
        self.value = value

    def to_expression(self, model):
        return getattr(model, self.attribute) == self.value

class GreaterThanSpecification(Specification):
    def __init__(self, attribute: str, value: Any):
        self.attribute = attribute
        self.value = value

    def to_expression(self, model):
        return getattr(model, self.attribute) > self.value

class ContainsSpecification(Specification):
    def __init__(self, attribute: str, value: str):
        self.attribute = attribute
        self.value = value

    def to_expression(self, model):
        return getattr(model, self.attribute).like(f"%{self.value}%")

# Composite Specifications
class AndSpecification(Specification):
    def __init__(self, *specifications: Specification):
        self.specifications = specifications

    def to_expression(self, model):
        from sqlalchemy import and_
        expressions = [spec.to_expression(model) for spec in self.specifications]
        return and_(*expressions)

class OrSpecification(Specification):
    def __init__(self, *specifications: Specification):
        self.specifications = specifications

    def to_expression(self, model):
        from sqlalchemy import or_
        expressions = [spec.to_expression(model) for spec in self.specifications]
        return or_(*expressions)

# Use Case Class with Inner Join
class ProductFilterUseCase:
    def __init__(self, session):
        self.session = session

    def filter_products(self, specification: Specification) -> List[Product]:
        query = self.session.query(Product).join(Category).filter(specification.to_expression(Product))
        return query.all()

    def filter_products_join_category(self, product_specification: Specification, category_specification: Specification) -> List[Product]:
      query = self.session.query(Product).join(Category)
      product_expression = product_specification.to_expression(Product)
      category_expression = category_specification.to_expression(Category)
      combined_expression = AndSpecification(product_expression, category_expression).to_expression(Product)
      query = query.filter(combined_expression)
      return query.all()

# Ví dụ sử dụng
if __name__ == "__main__":
    # Khởi tạo database và session
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Tạo dữ liệu mẫu
    electronics = Category(name="Electronics")
    books = Category(name="Books")
    session.add_all([
        electronics,
        books,
        Product(name="Laptop Dell XPS 13", category=electronics, price=1200),
        Product(name="Laptop HP Spectre x360", category=electronics, price=1500),
        Product(name="The Lord of the Rings", category=books, price=30),
        Product(name="Harry Potter", category=books, price=25),
    ])
    session.commit()

    # Tạo các specifications
    name_spec = EqualsSpecification("name", "Laptop Dell XPS 13")
    price_spec = GreaterThanSpecification("price", 1000)
    category_name_spec = EqualsSpecification("name", "Electronics")
    category_contains_spec = ContainsSpecification("name", "Electro")
    book_category_spec = EqualsSpecification("name", "Books")

    # Sử dụng Use Case với inner join và filter trên cả hai bảng
    filter_use_case = ProductFilterUseCase(session)
    filtered_products_join = filter_use_case.filter_products_join_category(price_spec, category_contains_spec)

    print("Filtered products with join:")
    for product in filtered_products_join:
        print(f"Name: {product.name}, Category: {product.category.name}, Price: {product.price}")

    #Filter product based on category name
    filtered_products_by_category = filter_use_case.filter_products_join_category(EqualsSpecification("id", 1), category_name_spec)
    print("Filter product based on category name:")
    for product in filtered_products_by_category:
        print(f"Name: {product.name}, Category: {product.category.name}, Price: {product.price}")

    filtered_products_book_category = filter_use_case.filter_products_join_category(EqualsSpecification("id", 3), book_category_spec)
    print("Filter product based on book category:")
    for product in filtered_products_book_category:
        print(f"Name: {product.name}, Category: {product.category.name}, Price: {product.price}")

    session.close()