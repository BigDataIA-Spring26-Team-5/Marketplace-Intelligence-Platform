"""Loads domain enrichment rules from domain_packs/<domain>/enrichment_rules.yaml."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DOMAIN_PACKS_DIR = PROJECT_ROOT / "domain_packs"


@dataclass
class PatternRule:
    pattern: re.Pattern
    label: str


@dataclass
class FieldRule:
    name: str
    strategy: str          # "deterministic" | "llm"
    output_type: str       # "single" | "multi" | "boolean"
    patterns: list[PatternRule] = field(default_factory=list)
    classification_classes: list[str] = field(default_factory=list)
    rag_context_field: Optional[str] = None


class EnrichmentRulesLoader:
    """Loads and parses enrichment_rules.yaml for a domain.

    Exposes:
      - all_fields: every FieldRule in declaration order
      - deterministic_fields: only strategy == "deterministic"
      - llm_fields: only strategy == "llm"
      - s1_fields: all fields that have patterns (both deterministic and llm with patterns)
    """

    def __init__(self, domain: str) -> None:
        self.domain = domain
        rules_path = DOMAIN_PACKS_DIR / domain / "enrichment_rules.yaml"

        if not rules_path.exists():
            logger.warning(
                "enrichment_rules.yaml not found for domain '%s' — no enrichment rules loaded",
                domain,
            )
            self.all_fields: list[FieldRule] = []
        else:
            self.all_fields = self._load(rules_path)

    def _load(self, path: Path) -> list[FieldRule]:
        with open(path) as f:
            data = yaml.safe_load(f)

        rules: list[FieldRule] = []
        for entry in data.get("fields", []):
            patterns = []
            for p in entry.get("patterns", []):
                try:
                    compiled = re.compile(p["regex"], re.I)
                    patterns.append(PatternRule(pattern=compiled, label=p["label"]))
                except re.error as exc:
                    logger.warning("Invalid regex in enrichment_rules for field '%s': %s", entry.get("name"), exc)

            rules.append(FieldRule(
                name=entry["name"],
                strategy=entry["strategy"],
                output_type=entry.get("output_type", "single"),
                patterns=patterns,
                classification_classes=entry.get("classification_classes", []),
                rag_context_field=entry.get("rag_context_field"),
            ))

        return rules

    @property
    def deterministic_fields(self) -> list[FieldRule]:
        return [f for f in self.all_fields if f.strategy == "deterministic"]

    @property
    def llm_fields(self) -> list[FieldRule]:
        return [f for f in self.all_fields if f.strategy == "llm"]

    @property
    def s1_fields(self) -> list[FieldRule]:
        """All fields with patterns — used by deterministic_enrich() S1 pass."""
        return [f for f in self.all_fields if f.patterns]

    def safety_field_names(self) -> list[str]:
        """Names of deterministic-only fields (must never go to S2/S3)."""
        return [f.name for f in self.deterministic_fields]
