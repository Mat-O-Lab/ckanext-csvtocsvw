import re

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.lib.jobs import DEFAULT_QUEUE_NAME
from ckan.lib.plugins import DefaultTranslation

from ckan import model

from ckanext.csvtocsvw.tasks import update_zip, annotate_csv

from ckanext.csvtocsvw import action
from ckanext.csvtocsvw import helpers

log = __import__('logging').getLogger(__name__)


class CsvtocsvwPlugin(plugins.SingletonPlugin, DefaultTranslation):
    plugins.implements(plugins.ITranslation)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IDomainObjectModification)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IPackageController, inherit=True)
    plugins.implements(plugins.IActions)
    #plugins.implements(plugins.IAuthFunctions)
    

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'csvtocsvw')

    # IDomainObjectModification

    def notify(self, entity, operation):
        u'''
        Send a notification on entity modification.

        :param entity: instance of module.Package.
        :param operation: 'new', 'changed' or 'deleted'.
        '''
        if operation == 'deleted':
            return

        log.debug(u'notify: {} {} \'{}\''
                  .format(operation, type(entity).__name__, entity.name))
        if isinstance(entity, model.Resource):
            log.debug('new uploaded resource')    
            dataset = entity.related_packages()[0]
            if entity.format=='CSV':
                log.debug('plugin notify event for resource: {}'.format(entity.id))
                enqueue_csvw_annotate(entity.id, entity.name, entity.url, dataset.id, operation)
        else:
            return

    # ITemplateHelpers

    def get_helpers(self):
        return helpers.get_helpers()
    
    # def get_auth_functions(self) -> dict[str, AuthFunction]:
    #     return {
    #         u'datapusher_submit': auth.datapusher_submit,
    #         u'datapusher_status': auth.datapusher_status
    #     }
    # IPackageController

    # def before_index(self, pkg_dict):
    #     try:
    #         if u'All resource data' in pkg_dict['res_name']:
    #             # we've got a 'Download all zip', so remove it's ZIP from the
    #             # SOLR facet of resource formats, as it's not really a data
    #             # resource
    #             pkg_dict['res_format'].remove('ZIP')
    #     except KeyError:
    #         # this happens when you save a new package without a resource yet
    #         pass
    #     return pkg_dict

    # IActions

    def get_actions(self):
        actions = {}
        if plugins.get_plugin('datastore'):
            # datastore is enabled, so we need to chain the datastore_create
            # action, to update the zip when it is called
            actions['datastore_create'] = action.datastore_create
        #actions['resource_update'] = action.resource_update
        return actions


# def enqueue_update_zip(dataset_name, dataset_id, operation):
#     # skip task if the dataset is already queued
#     queue = DEFAULT_QUEUE_NAME
#     jobs = toolkit.get_action('job_list')(
#         {'ignore_auth': True}, {'queues': [queue]})
#     if jobs:
#         for job in jobs:
#             if not job['title']:
#                 continue
#             match = re.match(
#                 r'DownloadAll \w+ "[^"]*" ([\w-]+)', job[u'title'])
#             if match:
#                 queued_dataset_id = match.groups()[0]
#                 if dataset_id == queued_dataset_id:
#                     log.info('Already queued dataset: {} {}'
#                              .format(dataset_name, dataset_id))
#                     return

#     # add this dataset to the queue
#     log.debug(u'Queuing job update_zip: {} {}'
#               .format(operation, dataset_name))

#     toolkit.enqueue_job(
#         update_zip, [dataset_id],
#         title=u'CSVtoCSVW {} "{}" {}'.format(operation, dataset_name,
#                                                dataset_id),
#         queue=queue)

#     # toolkit.enqueue_job(
#     #     "test", [dataset_id],
#     #     title=u'DownloadAll {} "{}" {}'.format(operation, dataset_name,
#     #                                            dataset_id),
#     #     queue=queue)

def enqueue_csvw_annotate(res_id, res_name, res_url, dataset_id, operation):
    # skip task if the dataset is already queued
    queue = DEFAULT_QUEUE_NAME
    jobs = toolkit.get_action('job_list')(
        {'ignore_auth': True}, {'queues': [queue]})
    log.debug("jobs")
    log.debug(jobs)
            
    if jobs:
        for job in jobs:
            if not job['title']:
                continue
            match = re.match(r'CSVtoCSVW \w+ "[^"]*" ([\w-]+)', job[u'title'])
            log.debug("match")
            log.debug(match)
            
            if match:
                queued_resource_id = match.groups()[0]
                if res_id == queued_resource_id:
                    log.info('Already queued resource: {} {}'
                             .format(res_name, res_id))
                    return

    # add this dataset to the queue
    log.debug(u'Queuing job csvw_annotate: {} {}'
              .format(operation, res_name))
    toolkit.enqueue_job(
        annotate_csv, [res_url, res_id, dataset_id],
        title=u'CSVtoCSVW {} "{}" {}'.format(operation, res_name, res_url),
        queue=queue)
