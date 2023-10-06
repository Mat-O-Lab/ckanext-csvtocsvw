import ckan.plugins as p
from ckan import model

from ckanext.csvtocsvw import plugin

log = __import__('logging').getLogger(__name__)

@p.toolkit.chained_action  # requires CKAN 2.7+
def datastore_create(original_action, context, data_dict):
#     # This gets called when xloader or datapusher loads a new resource or
#     # data dictionary is changed. We need to regenerate the zip when the latter
#     # happens, and it's ok if it happens at the other times too.
    result = original_action(context, data_dict)
#     if 'resource_id' in data_dict:
#         res = model.Resource.get(data_dict['resource_id'])
#         #datapusher finished if True
#         datapush_finished=result.get('calculate_record_count',False)   
#         if res and ('CSV' in res.format) and datapush_finished:
#             dataset = res.related_packages()[0]
#             log.debug('chained_action: enquery annotation job')
#             #plugin.enqueue_csvw_annotate(res.id, res.name, res.url, dataset.id, 'datastore_create')
    return result
    