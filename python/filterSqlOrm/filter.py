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

# Chuyển thành dict
dict_output = filter_request.dict()