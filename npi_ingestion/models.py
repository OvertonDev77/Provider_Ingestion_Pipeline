from pydantic import BaseModel
from typing import Optional

class Provider(BaseModel):
    npi_number: str
    organization_name: Optional[str]
    address: Optional[str]
    city: Optional[str]
    state: Optional[str]
    postal_code: Optional[str]
    phone: Optional[str]
    taxonomy_code: Optional[str]
    taxonomy_desc: Optional[str]
    authorized_official: Optional[str]
    last_updated: Optional[str] 