"""Tests for views.py."""

import pytest

#import ckanext.csvtocsvw.validators as validators


import ckan.plugins.toolkit as tk


@pytest.mark.ckan_config("ckan.plugins", "csvtocsvw")
@pytest.mark.usefixtures("with_plugins")
def test_csvtocsvw_blueprint(app, reset_db):
    resp = app.get(tk.h.url_for("csvtocsvw.page"))
    assert resp.status_code == 200
    assert resp.body == "Hello, csvtocsvw!"
