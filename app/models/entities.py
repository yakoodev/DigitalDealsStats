from datetime import datetime
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class AnalysisRequest(Base):
    __tablename__ = "analysis_requests"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    status: Mapped[str] = mapped_column(String(24), default="pending", index=True)
    query: Mapped[str] = mapped_column(String(512), nullable=False)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    force_refresh: Mapped[bool] = mapped_column(Boolean, default=False)
    result_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class AnalysisCache(Base):
    __tablename__ = "analysis_cache"
    __table_args__ = (UniqueConstraint("cache_key_hash", name="uq_analysis_cache_key_hash"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cache_key_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    query: Mapped[str] = mapped_column(String(512), nullable=False)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    valid_until: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    response_json: Mapped[dict] = mapped_column(JSON, nullable=False)


class AnalysisHistory(Base):
    __tablename__ = "analysis_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    request_id: Mapped[str] = mapped_column(String(36), index=True)
    query: Mapped[str] = mapped_column(String(512), nullable=False)
    query_normalized: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    options_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    result_json: Mapped[dict] = mapped_column(JSON, nullable=False)


class SectionCoverage(Base):
    __tablename__ = "section_coverage"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    request_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("analysis_requests.id", ondelete="CASCADE"),
        index=True,
    )
    section_url: Mapped[str] = mapped_column(String(1024), nullable=False)
    section_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    counter_total: Mapped[int | None] = mapped_column(Integer, nullable=True)
    loaded_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    coverage_status: Mapped[str] = mapped_column(String(32), nullable=False, default="full")


class OfferSnapshot(Base):
    __tablename__ = "offer_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    request_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("analysis_requests.id", ondelete="CASCADE"),
        index=True,
    )
    section_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    offer_id: Mapped[int] = mapped_column(Integer, index=True)
    seller_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    seller_name: Mapped[str] = mapped_column(String(256), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    price: Mapped[float] = mapped_column(Numeric(14, 4), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    reviews_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    seller_age: Mapped[str | None] = mapped_column(String(64), nullable=True)
    is_online: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    auto_delivery: Mapped[bool | None] = mapped_column(Boolean, nullable=True)


class ReviewSnapshot(Base):
    __tablename__ = "review_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    request_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("analysis_requests.id", ondelete="CASCADE"),
        index=True,
    )
    seller_id: Mapped[int] = mapped_column(Integer, index=True)
    detail: Mapped[str] = mapped_column(Text, nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False, default="")
    rating: Mapped[int | None] = mapped_column(Integer, nullable=True)
    date_bucket: Mapped[str | None] = mapped_column(String(64), nullable=True)
    is_relevant: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


Index("ix_offer_snapshots_request_offer", OfferSnapshot.request_id, OfferSnapshot.offer_id)
Index("ix_review_snapshots_request_seller", ReviewSnapshot.request_id, ReviewSnapshot.seller_id)
Index(
    "ix_analysis_history_lookup",
    AnalysisHistory.query_normalized,
    AnalysisHistory.currency,
    AnalysisHistory.options_hash,
    AnalysisHistory.generated_at,
)
