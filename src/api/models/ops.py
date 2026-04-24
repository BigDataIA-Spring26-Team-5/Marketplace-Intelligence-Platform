from __future__ import annotations

from pydantic import BaseModel, field_validator


class CacheStats(BaseModel):
    redis_connected: bool
    total_keys: int
    by_prefix: dict[str, int]
    sqlite_fallback: bool
    sqlite_key_count: int | None = None


class CacheFlushRequest(BaseModel):
    prefix: str | None = None
    domain: str | None = None
    confirm: bool

    @field_validator("confirm")
    @classmethod
    def must_confirm(cls, v: bool) -> bool:
        if not v:
            raise ValueError("confirm must be true to execute flush")
        return v


class CacheFlushResult(BaseModel):
    deleted_count: int
    prefix: str | None = None
    domain: str | None = None


class ColumnDef(BaseModel):
    name: str
    dtype: str
    required: bool = False
    enrichment: bool = False
    computed: bool = False


class SchemaResponse(BaseModel):
    domain: str
    columns: list[ColumnDef]
    source_file: str
