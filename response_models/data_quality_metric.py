from sqlite3 import Timestamp
from pydantic import BaseModel
from datetime import datetime

class DataQualityMetric(BaseModel):
    metric_id: str
    source_id: str
    metric_type: str
    metric_value: int
    recorded_at: datetime