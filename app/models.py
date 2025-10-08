from __future__ import annotations

from typing import List, Optional
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import String, Integer, Index, Text, ForeignKey


class Base(DeclarativeBase):
    pass


class Taxon(Base):
    __tablename__ = "taxon"
    __table_args__ = (
        Index("ix_taxon_gbif_key", "gbif_key"),
        Index("ix_taxon_accepted_gbif_key", "accepted_gbif_key"),
        Index("ix_taxon_status", "status"),
        Index("ix_taxon_rank", "rank"),
        Index("ix_taxon_family_genus", "family", "genus"),
        Index("ix_taxon_epiteto", "epiteto_especifico"),
        Index("ix_taxon_subfamily", "subfamily"),
        Index("ix_taxon_tribe", "tribe"),
        {
            "mysql_engine": "InnoDB",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_0900_ai_ci",
            "extend_existing": True,
        },
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    gbif_key: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    scientific_name: Mapped[str] = mapped_column(String(512), nullable=False)
    canonical_name: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    epiteto_especifico: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    authorship: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    rank: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    accepted_gbif_key: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # taxonomía principal
    kingdom: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    phylum: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    class_name: Mapped[Optional[str]] = mapped_column("class_name", String(64), nullable=True)
    order_name: Mapped[Optional[str]] = mapped_column("order_name", String(64), nullable=True)

    # extendida
    superfamily: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    family: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    subfamily: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    tribe: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    subtribe: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    genus: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    subgenus: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    iucn_category: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    sources_csv: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    provenance_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    synonyms: Mapped[List["Synonym"]] = relationship(
        "Synonym",
        back_populates="taxon",
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy="joined",
    )


class Synonym(Base):
    __tablename__ = "synonym"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    taxon_id: Mapped[int] = mapped_column(
        ForeignKey("taxon.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    authorship: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    status: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    source: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    external_key: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    rank: Mapped[Optional[str]] = mapped_column("taxon_rank", String(50), nullable=True)
    accepted_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    taxon: Mapped["Taxon"] = relationship("Taxon", back_populates="synonyms")
