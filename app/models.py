from pydantic import BaseModel
from typing import List, Optional

class SheetSelection(BaseModel):
    current_sheet: str
    previous_sheet: str
    increment_sheet: str
    master_sheet: str

    # Add optional headers for each sheet
    current_headers: Optional[List[str]] = None
    previous_headers: Optional[List[str]] = None
    increment_headers: Optional[List[str]] = None
    master_headers: Optional[List[str]] = None
