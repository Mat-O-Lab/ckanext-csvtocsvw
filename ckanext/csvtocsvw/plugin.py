import re, os

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan import model
from ckan.lib.plugins import DefaultTranslation

from ckan.types import Context
from typing import Any
import mimetypes

from ckanext.csvtocsvw import action, helpers, views, auth

log = __import__("logging").getLogger(__name__)

DEFAULT_FORMATS = os.environ.get("CKANINI__CSVTOCSVW__FORMATS", "").lower().split()
if not DEFAULT_FORMATS:
    DEFAULT_FORMATS = ["csv", "txt", "asc", "tsv"]


class CsvtocsvwPlugin(plugins.SingletonPlugin, DefaultTranslation):
    plugins.implements(plugins.ITranslation)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IResourceUrlChange)
    plugins.implements(plugins.IResourceController, inherit=True)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.IAuthFunctions)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, "templates")
        toolkit.add_public_directory(config_, "public")
        toolkit.add_resource("fanstatic", "csvtocsvw")
        mimetypes.add_type("application/ld+json", ".jsonld")

    # IResourceUrlChange

    def notify(self, resource: model.Resource):
        context: Context = {"ignore_auth": True}
        resource_dict = toolkit.get_action("resource_show")(
            context,
            {
                "id": resource.id,
            },
        )
        self._sumbit_toannotate(resource_dict)

    # IResourceController

    def after_resource_create(self, context: Context, resource_dict: dict[str, Any]):
        self._sumbit_toannotate(resource_dict)

    def after_update(self, context: Context, resource_dict: dict[str, Any]):
        self._sumbit_toannotate(resource_dict)

    def _sumbit_toannotate(self, resource_dict: dict[str, Any]):
        context = {"model": model, "ignore_auth": True, "defer_commit": True}
        format = resource_dict.get("format", None)
        submit = format and format.lower() in DEFAULT_FORMATS
        log.debug(
            "Submitting resource {0} with format {1}".format(
                resource_dict["id"], format
            )
            + " to csvtocsvw_annotate"
        )

        if not submit:
            return
        try:
            log.debug(
                "Submitting resource {0}".format(resource_dict["id"])
                + " to csvtocsvw_annotate"
            )
            toolkit.get_action("csvtocsvw_annotate")(
                context, {"id": resource_dict["id"]}
            )

        except toolkit.ValidationError as e:
            # If CSVTOCSVW is offline want to catch error instead
            # of raising otherwise resource save will fail with 500
            log.critical(e)
            pass
        return

    # ITemplateHelpers

    def get_helpers(self):
        return helpers.get_helpers()

    # IActions

    def get_actions(self):
        actions = action.get_actions()
        return actions

    # IBlueprint

    def get_blueprint(self):
        return views.get_blueprint()

    # IAuthFunctions

    def get_auth_functions(self):
        return auth.get_auth_functions()
