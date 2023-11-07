import re

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan import model
from ckan.lib.plugins import DefaultTranslation

from ckan.types import Context
from typing import Any

from ckanext.csvtocsvw import action, helpers

log = __import__("logging").getLogger(__name__)

DEFAULT_FORMATS = [
    "csv",
    "txt"
]


class CsvtocsvwPlugin(plugins.SingletonPlugin, DefaultTranslation):
    plugins.implements(plugins.ITranslation)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IResourceUrlChange)
    plugins.implements(plugins.IResourceController, inherit=True)
    plugins.implements(plugins.IActions)
    # plugins.implements(plugins.IAuthFunctions)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, "templates")
        toolkit.add_public_directory(config_, "public")
        toolkit.add_resource("fanstatic", "csvtocsvw")

    # # IDomainObjectModification

    # def notify(self, entity, operation):
    #     """
    #     Send a notification on entity modification.

    #     :param entity: instance of module.Package.
    #     :param operation: 'new', 'changed' or 'deleted'.
    #     """
    #     if operation == "deleted":
    #         return

    #     log.debug(
    #         "notify: {} {} '{}'".format(operation, type(entity).__name__, entity.name)
    #     )
    #     if isinstance(entity, model.Resource):
    #         log.debug("new uploaded resource")
    #         dataset = entity.related_packages()[0]
    #         if entity.format == "CSV":
    #             log.debug("plugin notify event for resource: {}".format(entity.id))
    #             enqueue_csvw_annotate(
    #                 entity.id, entity.name, entity.url, dataset.id, operation
    #             )
    #     else:
    #         return

    # IResourceUrlChange

    def notify(self, resource: model.Resource):
        context: Context = {'ignore_auth': True}
        resource_dict = p.toolkit.get_action(u'resource_show')(
            context, {
                u'id': resource.id,
            }
        )
        self._submit_to_datapusher(resource_dict)

    # IResourceController

    def after_resource_create(
            self, context: Context, resource_dict: dict[str, Any]):

        self._sumbit_toannotate(resource_dict)

    def after_update(
            self, context: Context, resource_dict: dict[str, Any]):

        self._sumbit_toannotate(resource_dict)

    
    def _sumbit_toannotate(self, resource_dict: dict[str, Any]):
        context = {
            u'model': model,
            u'ignore_auth': True,
            u'defer_commit': True
        }
        format=resource_dict.get('format',None)
        submit = (
            format
            and format.lower() in DEFAULT_FORMATS
        )
        log.debug(
                u'Submitting resource {0} with format {1}'.format(resource_dict['id'],format) +
                u' to csvwmapandtransform_transform'
            )
        
        if not submit:
            return
            
        try:
            log.debug(
                u'Submitting resource {0}'.format(resource_dict['id']) +
                u' to csvtocsvw_annotate'
            )
            toolkit.get_action('csvtocsvw_annotate')(context,{'id': resource_dict['id']})
             
        except toolkit.ValidationError as e:
            # If datapusher is offline want to catch error instead
            # of raising otherwise resource save will fail with 500
            log.critical(e)
            pass


    # ITemplateHelpers

    def get_helpers(self):
        return helpers.get_helpers()

    # IActions

    def get_actions(self):
        actions = action.get_actions()
        return actions
