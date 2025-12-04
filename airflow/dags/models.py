from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Literal
from datetime import date


class AviasalesRequestParams(BaseModel):
    """
    Pydantic is a model of request parameters for /aviasales/v3/prices_for_dates.
    """

    token: str = Field(..., description="Партнёрский токен API")
    origin: str = Field(
        ...,
        min_length=2,
        max_length=3,
        pattern=r"^[A-Z]{2,3}$",
        description="IATA код отправления",
    )
    destination: str = Field(
        ...,
        min_length=2,
        max_length=3,
        pattern=r"^[A-Z]{2,3}$",
        description="IATA код назначения",
    )
    currency: Literal["RUB", "USD", "EUR"] = "RUB"
    departure_at: date | None = Field(
        None, description="Дата вылета (YYYY-MM или YYYY-MM-DD)"
    )
    return_at: date | None = Field(
        None, description="Дата возврата. Для one_way=True — не указывать или None."
    )
    one_way: bool = Field(True, description="Билет в одну сторону")
    direct: bool = Field(False, description="Только прямые рейсы")
    market: str | None = Field(
        None, pattern=r"^[a-z]{2}$", description="Маркет (например, 'ru', 'us')"
    )
    limit: int = Field(30, ge=1, le=1000, description="Количество записей (макс. 1000)")
    page: int = Field(1, ge=1, description="Номер страницы (для пагинации)")
    sorting: Literal["price", "route"] = "price"
    unique: bool = Field(
        False, description="Только уникальные направления (если destination не задан)"
    )

    @field_validator("origin", "destination", mode="before")
    @classmethod
    def _upper_iata(cls, v: str) -> str:
        if not isinstance(v, str):
            raise ValueError("IATA код должен быть строкой")
        return v.upper()

    @model_validator(mode="after")
    def _validate_one_way_requires_no_return_at(self) -> "AviasalesRequestParams":
        if self.one_way and self.return_at is not None:
            raise ValueError(
                "For one_way=True, the return_at parameter must be None "
                "(according to the doc: 'to get one-way tickets, leave return_at empty')."
            )
        return self

    def to_query_dict(self) -> dict:
        """
        Converts the model into a dictionary of parameters for requests.get(params=...).
        Removes None values (except one_way/direct - they are strictly bool → str).
        """
        d = self.model_dump(exclude_none=True, exclude={"token"})

        d["one_way"] = str(self.one_way).lower()
        d["direct"] = str(self.direct).lower()
        d["unique"] = str(self.unique).lower()

        if self.departure_at:
            d["departure_at"] = self.departure_at.isoformat()
        if self.return_at:
            d["return_at"] = self.return_at.isoformat()

        return d
