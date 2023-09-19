from flask import Blueprint


csvtocsvw = Blueprint(
    "csvtocsvw", __name__)


def page():
    return "Hello, csvtocsvw!"


csvtocsvw.add_url_rule(
    "/csvtocsvw/page", view_func=page)


def get_blueprints():
    return [csvtocsvw]
