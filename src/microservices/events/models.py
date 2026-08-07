from typing import Optional, Any
from datetime import datetime
import uuid

from pydantic import BaseModel, Field


class Event(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: str
    timestamp: datetime = Field(default_factory=datetime.now)
    payload: dict


class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: str  # viewed, rated, added_to_favorites, etc.
    user_id: Optional[int] = None
    rating: Optional[float] = None
    genres: Optional[list[str]] = None
    description: Optional[str] = None


class UserEvent(BaseModel):
    user_id: int
    username: Optional[str] = None
    email: Optional[str] = None
    action: str  # registered, logged_in, updated_profile, etc.
    timestamp: datetime = Field(default_factory=datetime.now)


class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str  # completed, failed, refunded
    timestamp: datetime = Field(default_factory=datetime.now)
    method_type: Optional[str] = None


class EventResponse(BaseModel):
    status: str = "success"
    topic: str
    partition: int
    offset: int
    event_id: str