from pydantic import BaseModel, field_validator
from typing import Optional, Literal, Any


class ContentBlock(BaseModel):
    type: Literal["paragraphe", "image", "heading"]
    content: str | None = None


class ArticleSchema(BaseModel):
    id: str
    source: str
    title: str
    date: Optional[str] = None
    url: str
    label: str
    main_image: Optional[str] = None
    content_blocks: list[ContentBlock] = []

    @field_validator("date", mode="before")
    @classmethod
    def coerce_date(cls, v: Any) -> Optional[str]:
        """Accepte str, datetime, pd.Timestamp ou None et retourne toujours une str ISO ou None."""
        if v is None:
            return None
        import pandas as pd
        if isinstance(v, pd.Timestamp):
            return v.isoformat()
        if hasattr(v, "isoformat"):  # datetime.datetime
            return v.isoformat()
        return str(v) if str(v).strip() else None

    @field_validator("label", mode="before")
    @classmethod
    def coerce_label(cls, v: Any) -> str:
        """Accepte bool (True/False) ou str."""
        if isinstance(v, bool):
            return "Vrai" if v else "Faux"
        return str(v)