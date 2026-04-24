"""Property-based tests via Hypothesis — invariants across random inputs."""

from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given, strategies as st, settings, HealthCheck

from src.cache.client import CacheClient, _KNOWN_PREFIXES

_cc = CacheClient(no_cache=True)
_make_key = _cc._make_key
from src.uc3_search.indexer import _build_text, _tokenize
from src.blocks.strip_whitespace import StripWhitespaceBlock
from src.blocks.lowercase_brand import LowercaseBrandBlock


# ---------------------------------------------------------------------------
# Cache key generation invariants
# ---------------------------------------------------------------------------

@given(
    prefix=st.sampled_from(list(_KNOWN_PREFIXES)),
    payload=st.one_of(
        st.text(),
        st.lists(st.text(), max_size=10),
    ),
)
def test_make_key_deterministic(prefix, payload):
    """Same prefix+payload always yields same key."""
    k1 = _make_key(prefix, payload)
    k2 = _make_key(prefix, payload)
    assert k1 == k2


@given(
    prefix=st.sampled_from(list(_KNOWN_PREFIXES)),
    payload=st.text(min_size=1),
)
def test_make_key_format(prefix, payload):
    """Keys follow `prefix:<16-hex>` format."""
    key = _make_key(prefix, payload)
    assert key.startswith(f"{prefix}:")
    suffix = key.split(":", 1)[1]
    assert len(suffix) == 16
    assert all(c in "0123456789abcdef" for c in suffix)


@given(
    p1=st.sampled_from(list(_KNOWN_PREFIXES)),
    p2=st.sampled_from(list(_KNOWN_PREFIXES)),
    payload=st.text(min_size=1),
)
def test_make_key_prefix_isolation(p1, p2, payload):
    """Different prefix = different key (hash different due to prefix inclusion)."""
    if p1 == p2:
        return
    assert _make_key(p1, payload) != _make_key(p2, payload)


# ---------------------------------------------------------------------------
# Tokenizer invariants (UC3 indexer)
# ---------------------------------------------------------------------------

@given(st.text())
def test_tokenize_returns_list(text):
    result = _tokenize(text)
    assert isinstance(result, list)
    assert all(isinstance(t, str) for t in result)


@given(st.text())
def test_tokenize_lowercase(text):
    for t in _tokenize(text):
        assert t == t.lower()


@given(st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789", min_size=1, max_size=30))
def test_tokenize_preserves_ascii_alphanumerics(text):
    """Pure ASCII alphanumerics form single token."""
    tokens = _tokenize(text)
    assert tokens == [text.lower()]


# ---------------------------------------------------------------------------
# _build_text invariants
# ---------------------------------------------------------------------------

@given(
    name=st.text(max_size=40),
    brand=st.text(max_size=40),
    cat=st.text(max_size=40),
)
def test_build_text_lowercase(name, brand, cat):
    row = {"product_name": name, "brand_name": brand, "primary_category": cat}
    out = _build_text(row)
    assert out == out.lower()


@given(
    row=st.dictionaries(
        keys=st.sampled_from([
            "product_name", "brand_name", "primary_category",
            "ingredients", "dietary_tags", "allergens", "recall_reason"
        ]),
        values=st.one_of(st.none(), st.text(max_size=30)),
        max_size=7,
    )
)
def test_build_text_never_raises(row):
    out = _build_text(row)
    assert isinstance(out, str)


# ---------------------------------------------------------------------------
# StripWhitespaceBlock invariants
# ---------------------------------------------------------------------------

@given(
    data=st.lists(
        st.one_of(st.none(), st.text(max_size=30)),
        min_size=1, max_size=20,
    )
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_strip_whitespace_preserves_row_count(data):
    df = pd.DataFrame({"product_name": data, "brand_name": data})
    block = StripWhitespaceBlock()
    out = block.run(df)
    assert len(out) == len(df)


@given(
    val=st.text(max_size=30),
    lpad=st.integers(min_value=0, max_value=10),
    rpad=st.integers(min_value=0, max_value=10),
)
def test_strip_whitespace_idempotent(val, lpad, rpad):
    """Running twice == running once."""
    padded = " " * lpad + val + " " * rpad
    df = pd.DataFrame({"product_name": [padded]})
    block = StripWhitespaceBlock()
    once = block.run(df.copy())
    twice = block.run(once.copy())
    assert once["product_name"].tolist() == twice["product_name"].tolist()


# ---------------------------------------------------------------------------
# LowercaseBrandBlock invariants
# ---------------------------------------------------------------------------

@given(st.text(max_size=40))
def test_lowercase_brand_idempotent(val):
    df1 = pd.DataFrame({"brand_name": [val]})
    block = LowercaseBrandBlock()
    once = block.run(df1)
    twice = block.run(once.copy())
    assert once["brand_name"].tolist() == twice["brand_name"].tolist()


@given(
    brands=st.lists(st.text(max_size=30), min_size=1, max_size=10)
)
def test_lowercase_brand_output_is_lower(brands):
    df = pd.DataFrame({"brand_name": brands})
    block = LowercaseBrandBlock()
    out = block.run(df)
    for v in out["brand_name"].dropna():
        if isinstance(v, str):
            assert v == v.lower()
