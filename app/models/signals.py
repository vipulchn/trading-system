from datetime import datetime
from decimal import Decimal
from typing import Literal, Optional
from uuid import UUID, uuid4
from pydantic import BaseModel, Field, field_validator


class TVWebhookPayload(BaseModel):
    secret: str
    symbol: str
    exchange: str
    direction: Literal["Long", "Short"]
    setup_id: int
    entry_price: Decimal
    stop_price: Decimal
    target_1: Decimal
    target_2: Decimal
    vrvp_poc: Decimal
    vrvp_vah: Decimal
    vrvp_val: Decimal
    vrvp_level_used: Literal["POC", "VAH", "VAL", "HVN", "LVN"]
    svp_poc: Decimal
    svp_vah: Decimal
    svp_val: Decimal
    svp_alignment: Literal["Confirming", "Neutral", "Against"]
    day_type: Literal["Normal", "Trend", "Gap", "Neutral"]
    volume_ratio: Decimal
    bar_time: datetime
    rr_ratio: Decimal

    @field_validator("symbol")
    @classmethod
    def strip_exchange_prefix(cls, v: str) -> str:
        return v.split(":")[-1].upper()


class EnrichedSignal(BaseModel):
    signal_id: UUID = Field(default_factory=uuid4)
    received_at: datetime = Field(default_factory=datetime.utcnow)
    symbol: str
    direction: Literal["Long", "Short"]
    setup_id: int
    entry_price: Decimal
    stop_price: Decimal
    target_1: Decimal
    target_2: Decimal
    vrvp_level_used: str
    svp_alignment: str
    day_type: str
    volume_ratio: Decimal
    rr_ratio: Decimal
    capital_at_signal: Decimal
    status: str = "PENDING_RISK_REVIEW"


class ExecutionSignal(EnrichedSignal):
    quantity: int
    risk_amount_inr: Decimal
    status: str = "APPROVED"


class DailyCandidate(BaseModel):
    symbol: str
    day_type: Literal["Normal", "Trend", "Gap", "Neutral"]
    bias: Literal["Long", "Short"]
    setup_probability_score: int = Field(ge=1, le=10)
    key_level: Optional[Decimal] = None
    invalidation_level: Optional[Decimal] = None
    reasoning: Optional[str] = None
