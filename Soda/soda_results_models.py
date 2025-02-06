from pydantic import BaseModel
from typing import List


class CheckResult(BaseModel):
  check_name: str
  check_status: str
  check_value: str

class CheckResults(BaseModel):
    results: List[CheckResult]