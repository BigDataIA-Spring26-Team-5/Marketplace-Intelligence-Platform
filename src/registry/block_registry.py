"""Block registry — discovers and loads pre-built transformation blocks.

Block sequences are driven by domain_packs/<domain>/block_sequence.yaml.
Adding a new domain requires only a new domain_packs/<domain>/ directory — zero
edits to this file.
"""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path

import yaml

from src.blocks.base import Block
from src.schema.models import UnifiedSchema
from src.blocks.strip_whitespace import StripWhitespaceBlock
from src.blocks.lowercase_brand import LowercaseBrandBlock
from src.blocks.remove_noise_words import RemoveNoiseWordsBlock
from src.blocks.strip_punctuation import StripPunctuationBlock
from src.blocks.keep_quantity_in_name import KeepQuantityInNameBlock
from src.blocks.fuzzy_deduplicate import FuzzyDeduplicateBlock
from src.blocks.column_wise_merge import ColumnWiseMergeBlock
from src.blocks.golden_record_select import GoldenRecordSelectBlock
from src.blocks.dq_score import DQScorePreBlock, DQScorePostBlock
from src.blocks.llm_enrich import LLMEnrichBlock
from src.blocks.schema_enforce import SchemaEnforceBlock

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GENERATED_BLOCKS_DIR = PROJECT_ROOT / "src" / "blocks" / "generated"
DOMAIN_PACKS_DIR = PROJECT_ROOT / "domain_packs"

# Kernel blocks only — no food-specific blocks
_BLOCKS: dict[str, Block] = {
    "strip_whitespace": StripWhitespaceBlock(),
    "lowercase_brand": LowercaseBrandBlock(),
    "remove_noise_words": RemoveNoiseWordsBlock(),
    "strip_punctuation": StripPunctuationBlock(),
    "keep_quantity_in_name": KeepQuantityInNameBlock(),
    "fuzzy_deduplicate": FuzzyDeduplicateBlock(),
    "column_wise_merge": ColumnWiseMergeBlock(),
    "golden_record_select": GoldenRecordSelectBlock(),
    "dq_score_pre": DQScorePreBlock(),
    "dq_score_post": DQScorePostBlock(),
    "llm_enrich": LLMEnrichBlock(),
    "schema_enforce": SchemaEnforceBlock(),
}

# Stage definitions — dedup_stage only; enrich_stage removed (domain packs list blocks individually)
_STAGES: dict[str, list[str]] = {
    "dedup_stage": ["fuzzy_deduplicate", "column_wise_merge", "golden_record_select"],
}

FALLBACK_SEQUENCE: list[str] = [
    "dq_score_pre",
    "__generated__",
    "strip_whitespace",
    "remove_noise_words",
    "dq_score_post",
]


class BlockNotFoundError(KeyError):
    """Raised at registry init when a block_sequence.yaml references an unknown block."""


def _load_domain_sequence(domain: str, sequence_key: str = "sequence") -> list[str]:
    """Read block sequence from domain_packs/<domain>/block_sequence.yaml.

    Returns FALLBACK_SEQUENCE with a warning if the file is absent.
    Raises BlockNotFoundError at call time if any listed name is unresolvable
    (checked by BlockRegistry.__init__ after custom blocks are discovered).
    """
    pack_file = DOMAIN_PACKS_DIR / domain / "block_sequence.yaml"
    if not pack_file.exists():
        logger.warning(
            "No block_sequence.yaml for domain '%s' — using FALLBACK_SEQUENCE", domain
        )
        return list(FALLBACK_SEQUENCE)

    with open(pack_file) as f:
        data = yaml.safe_load(f)

    seq = data.get(sequence_key) or data.get("sequence")
    if not seq:
        logger.warning(
            "block_sequence.yaml for domain '%s' missing '%s' key — using FALLBACK_SEQUENCE",
            domain,
            sequence_key,
        )
        return list(FALLBACK_SEQUENCE)

    return list(seq)


def _discover_generated_blocks() -> dict[str, Block]:
    """Discover and load dynamically generated blocks from src/blocks/generated/."""
    generated = {}

    if not GENERATED_BLOCKS_DIR.exists():
        GENERATED_BLOCKS_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created generated blocks directory: {GENERATED_BLOCKS_DIR}")
        return generated

    for domain_dir in GENERATED_BLOCKS_DIR.iterdir():
        if not domain_dir.is_dir():
            continue

        domain = domain_dir.name

        # Discover YAML-based DynamicMappingBlocks (only YAML files are used now)
        for yaml_file in domain_dir.glob("DYNAMIC_MAPPING_*.yaml"):
            try:
                from src.blocks.dynamic_mapping import DynamicMappingBlock

                block = DynamicMappingBlock(domain=domain, yaml_path=str(yaml_file))
                generated[block.name] = block
                logger.info(
                    f"Loaded YAML mapping block: {block.name} "
                    f"(domain: {domain}, file: {yaml_file.name})"
                )
            except Exception as e:
                logger.error(f"Failed to load YAML mapping from {yaml_file}: {e}")

    return generated


class BlockRegistry:
    """Registry of pre-built and dynamically generated transformation blocks."""

    _instance: "BlockRegistry | None" = None

    @classmethod
    def instance(cls) -> "BlockRegistry":
        """Return the singleton registry, creating it on first call."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Clear the singleton (for testing only)."""
        cls._instance = None

    def __init__(self) -> None:
        self.blocks = dict(_BLOCKS)
        self._load_generated_blocks()
        self._discover_domain_custom_blocks()

    def _load_generated_blocks(self) -> None:
        """Load dynamically generated blocks from disk."""
        generated = _discover_generated_blocks()
        self.blocks.update(generated)
        logger.info(
            f"BlockRegistry initialized with {len(self.blocks)} blocks "
            f"({len(generated)} generated)"
        )

    def _discover_domain_custom_blocks(self) -> None:
        """Scan domain_packs/*/custom_blocks/*.py and register Block subclasses.

        Blocks are registered under key '<domain>__<block.name>' to prevent
        cross-domain collisions.
        """
        if not DOMAIN_PACKS_DIR.exists():
            return

        for domain_dir in sorted(DOMAIN_PACKS_DIR.iterdir()):
            if not domain_dir.is_dir():
                continue
            custom_dir = domain_dir / "custom_blocks"
            if not custom_dir.is_dir():
                continue
            domain = domain_dir.name

            for py_file in sorted(custom_dir.glob("*.py")):
                if py_file.name.startswith("_"):
                    continue
                module_name = f"domain_packs.{domain}.custom_blocks.{py_file.stem}"
                try:
                    spec = importlib.util.spec_from_file_location(module_name, py_file)
                    if spec is None or spec.loader is None:
                        continue
                    mod = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(mod)  # type: ignore[union-attr]
                    for attr_name in dir(mod):
                        attr = getattr(mod, attr_name)
                        if (
                            isinstance(attr, type)
                            and issubclass(attr, Block)
                            and attr is not Block
                        ):
                            instance = attr()
                            key = f"{domain}__{instance.name}" if not instance.name.startswith(f"{domain}__") else instance.name
                            self.blocks[key] = instance
                            logger.info("Discovered custom block: %s (domain: %s)", key, domain)
                except Exception as exc:
                    logger.error("Failed to load custom block from %s: %s", py_file, exc)

    def refresh(self) -> None:
        """Re-discover generated blocks (call after register_blocks_node writes new files)."""
        self._load_generated_blocks()

    def register_block(self, block: Block) -> None:
        """Register a new block (e.g., generated by Agent 2)."""
        self.blocks[block.name] = block
        logger.info(f"Registered block: {block.name}")

    def get(self, name: str) -> Block:
        """Get a block by name. Raises KeyError if not found."""
        if name not in self.blocks:
            raise KeyError(
                f"Block '{name}' not found. Available: {list(self.blocks.keys())}"
            )
        return self.blocks[name]

    def is_stage(self, name: str) -> bool:
        """Check if name is a stage (composite block)."""
        return name in _STAGES

    def expand_stage(self, name: str) -> list[str]:
        """Expand a stage to its constituent blocks."""
        return _STAGES.get(name, [name])

    def list_blocks(self, domain: str | None = None) -> list[str]:
        """List available block names, optionally filtered by domain."""
        if domain is None:
            return list(self.blocks.keys())
        return [
            name
            for name, block in self.blocks.items()
            if block.domain in ("all", domain)
        ]

    def validate_sequence(self, sequence: list[str], domain: str) -> None:
        """Raise BlockNotFoundError if any name in sequence is unresolvable.

        Valid entries: kernel block names, stage names, '__generated__', or
        namespaced custom block keys (already registered by this point).
        """
        valid_names = set(self.blocks.keys()) | set(_STAGES.keys()) | {"__generated__"}
        unknown = [
            name for name in sequence
            if name not in valid_names
        ]
        if unknown:
            raise BlockNotFoundError(
                f"Domain '{domain}' block_sequence references unknown blocks: {unknown}. "
                f"Available: {sorted(valid_names)}"
            )

    def get_default_sequence(
        self,
        domain: str = "nutrition",
        unified_schema: UnifiedSchema | None = None,
        enable_enrichment: bool = True,
    ) -> list[str]:
        """Return the block execution sequence for a domain.

        Reads domain_packs/<domain>/block_sequence.yaml. Falls back to
        FALLBACK_SEQUENCE when no domain pack exists.
        The __generated__ sentinel marks where agent-generated transforms are injected.
        """
        seq = _load_domain_sequence(domain, sequence_key="sequence")
        self.validate_sequence(seq, domain)
        return seq

    def get_silver_sequence(self, domain: str = "nutrition") -> list[str]:
        """Block sequence for Bronze→Silver: schema transform only.

        Reads the silver_sequence key from domain_packs/<domain>/block_sequence.yaml.
        Falls back to FALLBACK_SEQUENCE when absent.
        """
        return _load_domain_sequence(domain, sequence_key="silver_sequence")

    def get_gold_sequence(self, domain: str = "nutrition") -> list[str]:
        """Block sequence for Silver→Gold: dedup + enrichment + DQ.

        Reads the gold_sequence key from domain_packs/<domain>/block_sequence.yaml.
        Falls back to FALLBACK_SEQUENCE when absent.
        """
        return _load_domain_sequence(domain, sequence_key="gold_sequence")

    def get_blocks_with_metadata(self, block_names: list[str]) -> list[dict]:
        """Return metadata dicts for the given block names (preserving order).

        Handles stages by returning constituent block metadata.
        """
        result = []
        for name in block_names:
            if name == "__generated__":
                result.append(
                    {
                        "name": "__generated__",
                        "description": "Injection point for agent-generated transform blocks. MUST run after dq_score_pre and before normalization blocks.",
                        "inputs": ["source columns requiring schema transformation"],
                        "outputs": ["unified schema target columns"],
                    }
                )
                continue

            if self.is_stage(name):
                constituent_blocks = self.expand_stage(name)
                for block_name in constituent_blocks:
                    block = self.blocks.get(block_name)
                    if block:
                        result.append(
                            {
                                "name": block_name,
                                "description": f"[{name}] {block.description}",
                                "inputs": block.inputs,
                                "outputs": block.outputs,
                            }
                        )
                continue

            block = self.blocks.get(name)
            if block:
                result.append(
                    {
                        "name": name,
                        "description": block.description,
                        "inputs": block.inputs,
                        "outputs": block.outputs,
                    }
                )
        return result
