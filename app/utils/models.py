from typing import Any, Literal
from datetime import date
from pydantic import BaseModel, Field, field_validator, model_validator


class AviasalesRequestParams(BaseModel):
    token: str = Field(..., description="Partner API token")
    origin: str = Field(
        ...,
        min_length=2,
        max_length=3,
        pattern=r"^[A-Z]{2,3}$",
        description="Origin IATA code",
    )
    destination: str = Field(
        ...,
        min_length=2,
        max_length=3,
        pattern=r"^[A-Z]{2,3}$",
        description="Destination IATA code",
    )
    currency: Literal["RUB", "USD", "EUR"] = "RUB"
    # YYYY-MM or YYYY-MM-DD or python date
    departure_at: date | str | None = Field(
        None,
        description="Departure date (YYYY-MM or YYYY-MM-DD)",
    )
    return_at: date | str | None = Field(
        None,
        description="Return date (YYYY-MM or YYYY-MM-DD)",
    )
    one_way: bool = Field(True, description="One-way ticket flag")
    direct: bool = Field(False, description="Direct flights only flag")
    market: str | None = Field(
        None,
        pattern=r"^[a-z]{2}$",
        description="Market/country code",
    )
    limit: int = Field(
        30,
        ge=1,
        le=1000,
        description="Number of results to return",
    )
    page: int = Field(1, ge=1, description="Page number")
    sorting: Literal["price", "route"] = "price"
    unique: bool = Field(
        False,
        description="Return unique routes only",
    )

    @field_validator("origin", "destination", mode="before")
    @classmethod
    def _upper_iata(cls, v: Any) -> str:
        if not isinstance(v, str):
            raise ValueError("IATA code must be a string")
        return v.upper()

    @field_validator("departure_at", "return_at", mode="before")
    @classmethod
    def _normalize_date_or_month(cls, v: Any) -> Any:
        """Allow date or 'YYYY-MM' / 'YYYY-MM-DD' strings."""
        if v is None:
            return v
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            v = v.strip()
            # very light validation; API will do strict check
            if len(v) in (7, 10):  # 'YYYY-MM' or 'YYYY-MM-DD'
                return v
            raise ValueError(
                "departure_at/return_at must be date or string 'YYYY-MM'/'YYYY-MM-DD'"
            )
        raise ValueError("departure_at/return_at must be date or string")

    @model_validator(mode="after")
    def _validate_one_way_requires_no_return_at(self) -> "AviasalesRequestParams":
        if self.one_way and self.return_at is not None:
            raise ValueError("If one_way=True, return_at must be None")
        return self

    def to_query_dict(self) -> dict:
        """Build query params dict"""
        d = self.model_dump(exclude_none=True, exclude={"token"})

        d["one_way"] = str(self.one_way).lower()
        d["direct"] = str(self.direct).lower()
        d["unique"] = str(self.unique).lower()

        if self.departure_at is not None:
            if isinstance(self.departure_at, date):
                d["departure_at"] = self.departure_at.isoformat()
            else:
                d["departure_at"] = self.departure_at  # string as is

        if self.return_at is not None:
            if isinstance(self.return_at, date):
                d["return_at"] = self.return_at.isoformat()
            else:
                d["return_at"] = self.return_at  # string as is

        return d
