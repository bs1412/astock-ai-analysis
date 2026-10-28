"""One extraction contract shared by adapters, validation and the typed interface."""
from typing import Literal
from pydantic import BaseModel,ConfigDict,Field


class StrictModel(BaseModel):
    model_config=ConfigDict(extra='forbid')


class Evidence(StrictModel):
    start:int|None=Field(default=None,ge=0)
    end:int|None=Field(default=None,gt=0)
    quote:str=Field(min_length=1,max_length=600)


class Fact(StrictModel):
    value:str|None=Field(default=None,max_length=300)
    evidence:Evidence|None=None


class Quantity(StrictModel):
    kind:Literal['issued_total','initial_listed','cash_amount','other']
    value:str=Field(min_length=1,max_length=50)
    unit:Literal['份','万份','亿份','股','万股','亿股','元','万元','亿元']
    currency:Literal['CNY','USD','HKD']|None=None
    normalized_value:str|None=None
    normalized_unit:str|None=None
    evidence:Evidence


class Candidate(StrictModel):
    source_url:str=Field(max_length=2048)
    security_code:Fact
    market:Fact
    event_type:Fact
    published_date:Fact
    event_date:Fact=Field(default_factory=Fact)
    report_period:Fact=Field(default_factory=Fact)
    quantities:list[Quantity]=Field(default_factory=list,max_length=20)


class Submit(StrictModel):
    source_url:str=Field(max_length=2048)
    idempotency_key:str=Field(min_length=8,max_length=128)
    mode:Literal['baseline','model']='baseline'
    consent_paid_call:bool=False
    use_sample:bool=False
    text:str|None=Field(default=None,max_length=100000)


class Resume(StrictModel):
    acknowledge_uncertain_call:bool=False


class Revision(StrictModel):
    candidate:Candidate
    reason:str=Field(min_length=3,max_length=1000)


class Review(StrictModel):
    decision:Literal['accepted','rejected']
    note:str=Field(min_length=3,max_length=1000)
