import ckan.plugins as p
from ckan import model
import ckan.plugins.toolkit as toolkit
from ckan.types import Context
from ckan.lib.jobs import DEFAULT_QUEUE_NAME
from typing import Any
from ckanext.csvtocsvw.tasks import annotate_csv, transform_csv
import datetime
from dateutil.parser import parse as parse_date
from dateutil.parser import isoparse as parse_iso_date
import json
import re

log = __import__("logging").getLogger(__name__)


def csvtocsvw_annotate(context: Context, data_dict: dict[str, Any]) -> dict[str, Any]:
    """Start a the transformation job for a certain resource.

    :param resource_id: The resource id of the resource that you want the
        datapusher status for.
    :type resource_id: string
    """

    if "id" in data_dict:
        data_dict["resource_id"] = data_dict["id"]
    res_id = toolkit.get_or_bust(data_dict, "resource_id")
    resource = toolkit.get_action("resource_show")(
        {"ignore_auth": True}, {"id": res_id}
    )
    data_dict["resource"] = resource
    toolkit.check_access("csvtocsvw_annotate", context, data_dict)

    task = {
        "entity_id": res_id,
        "entity_type": "resource",
        "task_type": "csvtocsvw",
        "last_updated": str(datetime.datetime.utcnow()),
        "state": "submitting",
        "key": "csvtocsvw_annotate",
        "value": "{}",
        "error": "{}",
        "detail": "",
    }
    log.debug("annotation_started for: {}".format(resource))
    try:
        existing_task = toolkit.get_action("task_status_show")(
            {},
            {
                "entity_id": res_id,
                "task_type": "csvtocsvw",
                "key": "csvtocsvw_annotate",
            },
        )
        assume_task_stale_after = datetime.timedelta(seconds=3600)
        assume_task_stillborn_after = datetime.timedelta(seconds=int(5))
        if existing_task.get("state") == "pending":
            # queued_res_ids = [
            #     re.search(r"'resource_id': u?'([^']+)'",
            #               job.description).groups()[0]
            #     for job in get_queue().get_jobs()
            #     if 'xloader_to_datastore' in str(job)  # filter out test_job etc
            # ]
            updated = parse_iso_date(existing_task["last_updated"])
            time_since_last_updated = datetime.datetime.utcnow() - updated
            # if (res_id not in queued_res_ids
            #         and time_since_last_updated > assume_task_stillborn_after):
            #     # it's not on the queue (and if it had just been started then
            #     # its taken too long to update the task_status from pending -
            #     # the first thing it should do in the xloader job).
            #     # Let it be restarted.
            #     log.info('A pending task was found %r, but its not found in '
            #              'the queue %r and is %s hours old',
            #              existing_task['id'], queued_res_ids,
            #              time_since_last_updated)
            # elif time_since_last_updated > assume_task_stale_after:
            if time_since_last_updated > assume_task_stale_after:
                # it's been a while since the job was last updated - it's more
                # likely something went wrong with it and the state wasn't
                # updated than its still in progress. Let it be restarted.
                log.info(
                    "A pending task was found %r, but it is only %s hours" " old",
                    existing_task["id"],
                    time_since_last_updated,
                )
            else:
                log.info(
                    "A pending task was found %s for this resource, so "
                    "skipping this duplicate task",
                    existing_task["id"],
                )
                return None

        task["id"] = existing_task["id"]
    except toolkit.ObjectNotFound:
        pass
    res = enqueue_csvw_annotate(
        resource["id"],
        resource["name"],
        resource["url"],
        resource["package_id"],
        task["last_updated"],
    )
    value = json.dumps({"job_id": res.id})
    task["value"] = value
    task["state"] = "pending"
    toolkit.get_action("task_status_update")(
        {"session": model.meta.create_local_session(), "ignore_auth": True}, task
    )
    return res


def enqueue_csvw_annotate(
    res_id, res_name, res_url, dataset_id, task_last_updated, operation="changed"
):
    # skip task if the dataset is already queued
    queue = DEFAULT_QUEUE_NAME
    jobs = toolkit.get_action("job_list")({"ignore_auth": True}, {"queues": [queue]})
    if jobs:
        for job in jobs:
            if not job["title"]:
                continue
            match = re.match(r'csvtocsvw \w+ "[^"]*" ([\w-]+)', job["title"])
            log.debug("match")
            log.debug(match)

            if match:
                queued_resource_id = match.groups()[0]
                if res_id == queued_resource_id:
                    log.info("Already queued resource: {} {}".format(res_name, res_id))
                    return

    # add this dataset to the queue
    log.debug("Queuing job csvw_annotate: {}".format(res_name))
    callback_url = toolkit.url_for(
        "api.action", ver=3, logic_function="csvtocsvw_hook", qualified=True
    )
    job = toolkit.enqueue_job(
        annotate_csv,
        [res_url, res_id, dataset_id, callback_url, task_last_updated],
        title='csvtocsvw  {} "{}" {}'.format(operation, res_name, res_url),
        queue=queue,
    )
    return job


def csvtocsvw_transform(context: Context, data_dict: dict[str, Any]) -> dict[str, Any]:
    """Start a the transformation job for a certain resource.

    :param resource_id: The resource id of the resource that you want the
        datapusher status for.
    :type resource_id: string
    """

    if "id" in data_dict:
        data_dict["resource_id"] = data_dict["id"]
    res_id = toolkit.get_or_bust(data_dict, "resource_id")
    resource = toolkit.get_action("resource_show")(
        {"ignore_auth": True}, {"id": res_id}
    )
    data_dict["resource"] = resource
    toolkit.check_access("csvtocsvw_transform", context, data_dict)

    task = {
        "entity_id": res_id,
        "entity_type": "resource",
        "task_type": "csvtocsvw",
        "last_updated": str(datetime.datetime.utcnow()),
        "state": "submitting",
        "key": "csvtocsvw_transform",
        "value": "{}",
        "error": "{}",
        "detail": "",
    }
    log.debug("transform_started for: {}".format(resource))
    try:
        existing_task = toolkit.get_action("task_status_show")(
            {},
            {
                "entity_id": res_id,
                "task_type": "csvtocsvw",
                "key": "csvtocsvw_transform",
            },
        )
        assume_task_stale_after = datetime.timedelta(seconds=3600)
        assume_task_stillborn_after = datetime.timedelta(seconds=int(5))
        if existing_task.get("state") == "pending":
            # queued_res_ids = [
            #     re.search(r"'resource_id': u?'([^']+)'",
            #               job.description).groups()[0]
            #     for job in get_queue().get_jobs()
            #     if 'xloader_to_datastore' in str(job)  # filter out test_job etc
            # ]
            updated = parse_iso_date(existing_task["last_updated"])
            time_since_last_updated = datetime.datetime.utcnow() - updated
            # if (res_id not in queued_res_ids
            #         and time_since_last_updated > assume_task_stillborn_after):
            #     # it's not on the queue (and if it had just been started then
            #     # its taken too long to update the task_status from pending -
            #     # the first thing it should do in the xloader job).
            #     # Let it be restarted.
            #     log.info('A pending task was found %r, but its not found in '
            #              'the queue %r and is %s hours old',
            #              existing_task['id'], queued_res_ids,
            #              time_since_last_updated)
            # elif time_since_last_updated > assume_task_stale_after:
            if time_since_last_updated > assume_task_stale_after:
                # it's been a while since the job was last updated - it's more
                # likely something went wrong with it and the state wasn't
                # updated than its still in progress. Let it be restarted.
                log.info(
                    "A pending task was found %r, but it is only %s hours" " old",
                    existing_task["id"],
                    time_since_last_updated,
                )
            else:
                log.info(
                    "A pending task was found %s for this resource, so "
                    "skipping this duplicate task",
                    existing_task["id"],
                )
                return False

        task["id"] = existing_task["id"]
    except toolkit.ObjectNotFound:
        pass
    res = enqueue_csvw_transform(
        resource["id"],
        resource["name"],
        resource["url"],
        resource["package_id"],
        task["last_updated"],
    )
    value = json.dumps({"job_id": res.id})
    task["value"] = value
    task["state"] = "pending"
    toolkit.get_action("task_status_update")(
        # {'session': model.meta.create_local_session(), 'ignore_auth': True},
        {"ignore_auth": True},
        task,
    )
    return res


def enqueue_csvw_transform(
    res_id, res_name, res_url, dataset_id, task_last_updated, operation="changed"
):
    # skip task if the dataset is already queued
    queue = DEFAULT_QUEUE_NAME
    jobs = toolkit.get_action("job_list")({"ignore_auth": True}, {"queues": [queue]})
    if jobs:
        for job in jobs:
            if not job["title"]:
                continue
            match = re.match(r'csvtocsvw \w+ "[^"]*" ([\w-]+)', job["title"])
            log.debug("match")
            log.debug(match)

            if match:
                queued_resource_id = match.groups()[0]
                if res_id == queued_resource_id:
                    log.info("Already queued resource: {} {}".format(res_name, res_id))
                    return

    # add this dataset to the queue
    log.debug("Queuing job csvw_transform: {}".format(res_name))
    callback_url = toolkit.url_for(
        "api.action", ver=3, logic_function="csvtocsvw_hook", qualified=True
    )
    job = toolkit.enqueue_job(
        transform_csv,
        [res_url, res_id, dataset_id, callback_url, task_last_updated],
        title='csvtocsvw  {} "{}" {}'.format(operation, res_name, res_url),
        queue=queue,
    )
    return job


def csvtocsvw_hook(context, data_dict):
    """Update csvtocsvw task. This action is typically called by ckanext-csvtocsvw
    whenever the status of a job changes.

    :param metadata: metadata provided when submitting job. key-value pairs.
                     Must have resource_id property.
    :type metadata: dict
    :param status: status of the job from the csvwmapandtransform service. Allowed values:
                   pending, running, running_but_viewable, complete, error
                   (which must all be valid values for task_status too)
    :type status: string
    :param error: Error raised during job execution
    :type error: string

    NB here are other params which are in the equivalent object in
    ckan-service-provider (from job_status):
        :param sent_data: Input data for job
        :type sent_data: json encodable data
        :param job_id: An identifier for the job
        :type job_id: string
        :param result_url: Callback url
        :type result_url: url string
        :param data: Results from job.
        :type data: json encodable data
        :param requested_timestamp: Time the job started
        :type requested_timestamp: timestamp
        :param finished_timestamp: Time the job finished
        :type finished_timestamp: timestamp

    """
    log.debug("callback got {}".format(data_dict))
    metadata, status = toolkit.get_or_bust(data_dict, ["metadata", "status"])

    res_id = toolkit.get_or_bust(metadata, "resource_id")

    # Pass metadata, not data_dict, as it contains the resource id needed
    # on the auth checks
    # toolkit.check_access('xloader_submit', context, metadata)

    task = toolkit.get_action("task_status_show")(
        context,
        {"entity_id": res_id, "task_type": "csvtocsvw", "key": metadata["task_key"]},
    )

    task["state"] = status
    task["last_updated"] = str(datetime.datetime.utcnow())
    task["error"] = data_dict.get("error")
    resubmit = False

    if status in ("complete"):
        log.debug("callback job complete {}".format(task))

        # Create default views for resource if necessary (only the ones that
        # require data to be in the DataStore)
        resource_dict = toolkit.get_action("resource_show")(context, {"id": res_id})

        dataset_dict = toolkit.get_action("package_show")(
            context, {"id": resource_dict["package_id"]}
        )

        # Check if the uploaded file has been modified in the meantime
        if resource_dict.get("last_modified") and metadata.get("task_created"):
            try:
                last_modified_datetime = parse_date(resource_dict["last_modified"])
                task_created_datetime = parse_date(metadata["task_created"])
                if last_modified_datetime > task_created_datetime:
                    log.debug(
                        "Uploaded file more recent: %s > %s",
                        last_modified_datetime,
                        task_created_datetime,
                    )
                    resubmit = True
            except ValueError:
                pass
        # Check if the URL of the file has been modified in the meantime
        elif (
            resource_dict.get("url")
            and metadata.get("original_url")
            and resource_dict["url"] != metadata["original_url"]
        ):
            log.debug(
                "URLs are different: %s != %s",
                resource_dict["url"],
                metadata["original_url"],
            )
            resubmit = True

    context["ignore_auth"] = True
    toolkit.get_action("task_status_update")(context, task)

    # create default views
    # logic.get_action('resource_create_default_resource_views')(
    #         context,
    #         {
    #             'resource': resource_dict,
    #             'package': dataset_dict,
    #             'create_datastore_views': True,
    #         })

    if resubmit:
        log.debug("Resource %s has been modified, " "resubmitting to csvtocsvw", res_id)
        toolkit.get_action("csvtocsvwm_transform")(context, {"resource_id": res_id})


def get_actions():
    actions = {
        "csvtocsvw_annotate": csvtocsvw_annotate,
        "csvtocsvw_transform": csvtocsvw_transform,
        "csvtocsvw_hook": csvtocsvw_hook,
    }
    return actions
