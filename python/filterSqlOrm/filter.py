from pydantic import BaseModel
from typing import List

# Model cho filter con nhất (level thấp nhất)
class SubFilter(BaseModel):
    id: str
    name: str
    value: str

# Model cho filter group (chứa danh sách các sub filters)
class FilterGroup(BaseModel):
    id: str
    filters: List[SubFilter]

# Model chính chứa tất cả thông tin
class FilterRequest(BaseModel):
    page: int
    page_size: int
    filters: List[FilterGroup]

# Ví dụ sử dụng:
json_data = {
    "page": 1,
    "page_size": 100,
    "filters":[
        {
            "id": "abcdedf",
            "filters": [
                {
                    "id": "abc123",
                    "name": "abc",
                    "value": "asd"
                },
                {
                    "id": "abc124",
                    "name": "abd",
                    "value": "ase"
                }
            ]
        },
        {
            "id": "abcdedg",
            "filters": [
                {
                    "id": "abc125",
                    "name": "abc",
                    "value": "asd"
                },
                {
                    "id": "abc126",
                    "name": "abd",
                    "value": "ase"
                }
            ]
        }
    ]
}

# Parse JSON data thành Pydantic object
filter_request = FilterRequest.parse_obj(json_data)

# Bạn có thể truy cập dữ liệu như sau:
print(filter_request.page)  # 1
print(filter_request.page_size)  # 100
print(filter_request.filters[0].id)  # "abcdedf"
print(filter_request.filters[0].filters[0].name)  # "abc"

# Chuyển đổi ngược lại thành JSON
json_output = filter_request.json()

# Chuyển thành dict1
dict_output = filter_request.dict()

from abc import ABC, abstractmethod
from typing import List

# Các class cơ bản
class Item:
    def __init__(self, name, price):
        self.name = name
        self.price = price

    def get_price(self):
        return self.price

class OrderComponent(ABC): #Interface cho cả Item và Order
    @abstractmethod
    def get_price(self):
        pass

class Order(OrderComponent): #Composite
    def __init__(self):
        self.items: List[OrderComponent] = []

    def add(self, item: OrderComponent):
        self.items.append(item)

    def get_price(self):
        total_price = 0
        for item in self.items:
            total_price += item.get_price()
        return total_price

# Strategy Pattern
class PricingStrategy(ABC):
    @abstractmethod
    def calculate_price(self, item: Item):
        pass

class RegularPricing(PricingStrategy):
    def calculate_price(self, item: Item):
        return item.get_price()

class DiscountPricing(PricingStrategy):
    def __init__(self, discount_percent):
        self.discount_percent = discount_percent

    def calculate_price(self, item: Item):
        return item.get_price() * (1 - self.discount_percent / 100)

# Sử dụng kết hợp Composite và Strategy
order = Order()

item1 = Item("Áo thun", 100000)
item2 = Item("Quần jean", 200000)
item3 = Item("Giày", 500000)

# Áp dụng strategy cho từng item
regular_pricing = RegularPricing()
discount_pricing_10 = DiscountPricing(10)
discount_pricing_20 = DiscountPricing(20)

# Cách 1: Áp dụng strategy riêng cho từng item
order.add(item1)
order.add(item2)
order.add(item3)

total_price = 0
for item in order.items:
    if item.name == "Quần jean": # Ví dụ áp dụng giảm giá cho quần jean
      total_price += discount_pricing_10.calculate_price(item)
    elif item.name == "Giày":
      total_price += discount_pricing_20.calculate_price(item)
    else:
      total_price += regular_pricing.calculate_price(item)

print(f"Tổng giá trị đơn hàng (áp dụng strategy riêng): {total_price}")

#Cách 2: Áp dụng Strategy cho cả Order (thường ít dùng hơn trong trường hợp này)
class OrderPricingStrategy(ABC):
    @abstractmethod
    def calculate_order_price(self, order: Order):
        pass

class RegularOrderPricing(OrderPricingStrategy):
    def calculate_order_price(self, order: Order):
        total_price = 0
        for item in order.items:
            total_price += item.get_price()
        return total_price

class DiscountOrderPricing(OrderPricingStrategy):
    def __init__(self, discount_percent):
        self.discount_percent = discount_percent

    def calculate_order_price(self, order: Order):
        total_price = 0
        for item in order.items:
            total_price += item.get_price()
        return total_price * (1 - self.discount_percent/100)

order2 = Order()
order2.add(item1)
order2.add(item2)
order2.add(item3)
order_pricing = DiscountOrderPricing(5)
print(f"Tổng giá trị đơn hàng 2 (áp dụng strategy cho cả order): {order_pricing.calculate_order_price(order2)}")