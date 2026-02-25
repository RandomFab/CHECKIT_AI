from pydantic import BaseModel
from typing import Optional, Literal

class ContentBlock(BaseModel):
    type: Literal["paragraphe", "image", "heading"]
    content: str | None = None

class ArticalSchema(BaseModel):
    id: str
    source: str
    title: str
    date: Optional[str]
    url: str
    label: str
    main_image: Optional[str]
    content_blocks: list[ContentBlock] = []