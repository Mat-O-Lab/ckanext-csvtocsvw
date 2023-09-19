"""Tests for validators.py."""

import pytest

import ckan.plugins.toolkit as tk

from ckanext.csvtocsvw.logic import validators


def test_csvtocsvw_reauired_with_valid_value():
    assert validators.csvtocsvw_required("value") == "value"


def test_csvtocsvw_reauired_with_invalid_value():
    with pytest.raises(tk.Invalid):
        validators.csvtocsvw_required(None)
