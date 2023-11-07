import ckan.plugins as p
from ckan import model
import ckan.plugins.toolkit as toolkit
from ckan.types import Context
from ckan.lib.jobs import DEFAULT_QUEUE_NAME
from typing import Any
from ckanext.csvtocsvw.tasks import annotate_csv

log = __import__("logging").getLogger(__name__)

def csvtocsvw_annotate(
    context: Context, data_dict: dict[str, Any]) -> dict[str, Any]:
    ''' Start a the transformation job for a certain resource.

    :param resource_id: The resource id of the resource that you want the
        datapusher status for.
    :type resource_id: string
    '''

    #toolkit.check_access('csvwmapandtransform_transform_status', context, data_dict)

    if 'id' in data_dict:
        data_dict['resource_id'] = data_dict['id']
    res_id = toolkit.get_or_bust(data_dict, 'resource_id')
    resource = toolkit.get_action(u'resource_show'
                                          )({"ignore_auth": True}, {
                                              u'id': res_id
                                          })
    log.debug('transform_started for: {}'.format(resource))
    res=enqueue_csvw_annotate(resource['id'], resource['name'], resource['url'], resource['package_id'], operation='changed')
    return res

def get_actions():
    actions={
        'csvtocsvw_annotate': csvtocsvw_annotate
    }
    return actions


def enqueue_csvw_annotate(res_id, res_name, res_url, dataset_id, operation):
    # skip task if the dataset is already queued
    queue = DEFAULT_QUEUE_NAME
    jobs = toolkit.get_action("job_list")({"ignore_auth": True}, {"queues": [queue]})
    if jobs:
        for job in jobs:
            if not job["title"]:
                continue
            match = re.match(r'CSVtoCSVW \w+ "[^"]*" ([\w-]+)', job["title"])
            log.debug("match")
            log.debug(match)

            if match:
                queued_resource_id = match.groups()[0]
                if res_id == queued_resource_id:
                    log.info("Already queued resource: {} {}".format(res_name, res_id))
                    return

    # add this dataset to the queue
    log.debug("Queuing job csvw_annotate: {} {}".format(operation, res_name))
    toolkit.enqueue_job(
        annotate_csv,
        [res_url, res_id, dataset_id],
        title='CSVtoCSVW {} "{}" {}'.format(operation, res_name, res_url),
        queue=queue,
    )
