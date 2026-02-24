# tests/test_translation.py

from app.core import translate_sku

def test_translate_sku_none_and_empty():
    assert translate_sku(None) is None
    assert translate_sku("") is None
    assert translate_sku("   ") is None

def test_translate_sku_normalization_is_stable():
    # Same SKU should behave consistently across whitespace/case
    a = translate_sku("  AbC-123  ")
    b = translate_sku("abc-123")
    c = translate_sku("ABC-123")
    assert a == b == c

def test_translate_sku_returns_string_or_none():
    out = translate_sku("SOME-SKU-THAT-MAY-NOT-EXIST")
    assert (out is None) or isinstance(out, str)