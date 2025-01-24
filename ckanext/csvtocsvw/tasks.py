import datetime
import decimal
import json
from urllib.parse import urljoin, urlparse, urlsplit

import ckan.plugins.toolkit as toolkit
import ckanapi
import ckanapi.datapackage
import requests
from ckan import model
from requests_toolbelt.multipart.encoder import MultipartEncoder

from ckanext.csvtocsvw.annotate import (
    annotate_csv_upload,
    annotate_csv_uri,
    csvw_to_rdf,
)
from ckanext.csvtocsvw.csvw_parser import CSVWtoRDF, simple_columns

log = __import__("logging").getLogger(__name__)

CHUNK_INSERT_ROWS = 250


def annotate_csv(
    res_url, res_id, dataset_id, callback_url, last_updated, skip_if_no_changes=True
):
    # url = '{ckan}/dataset/{pkg}/resource/{res_id}/download/{filename}'.format(
    #         ckan=CKAN_URL, pkg=dataset_id, res_id=res_id, filename=res_url)
    context = {"session": model.meta.create_local_session(), "ignore_auth": True}
    CSVTOCSVW_TOKEN = toolkit.config.get("ckanext.csvtocsvw.ckan_token")
    SSL_VERIFY = toolkit.config.get("ckanext.csvtocsvw.ssl_verify")
    if not SSL_VERIFY:
        requests.packages.urllib3.disable_warnings()

    metadata = {
        "ckan_url": toolkit.config.get("ckan.site_url"),
        "resource_id": res_id,
        "task_created": last_updated,
        "original_url": res_url,
        "task_key": "csvtocsvw_annotate",
    }
    job_info = dict()
    job_dict = dict(metadata=metadata, status="running", job_info=job_info)
    errored = False
    callback_csvtocsvw_hook(callback_url, api_key=CSVTOCSVW_TOKEN, job_dict=job_dict)

    csv_res = toolkit.get_action("resource_show")(context, {"id": res_id})
    log.debug("Annotating: {}".format(csv_res["url"]))

    s = requests.Session()
    s.verify = SSL_VERIFY
    s.headers.update({"Authorization": CSVTOCSVW_TOKEN})
    csv_data = s.get(csv_res["url"]).content
    filename, meta_data, mime_type = annotate_csv_uri(
        csv_res["url"], authorization=CSVTOCSVW_TOKEN
    )
    if meta_data:
        prefix, suffix = filename.rsplit(".", 1)
        if suffix == "json" and "ld+json" in mime_type:
            log.debug(
                "{}.{} {} is json-ld:{}".format(
                    prefix, suffix, mime_type, "ld+json" in mime_type
                )
            )
            filename = prefix + ".jsonld"
        else:
            filename = prefix + "." + suffix

        metadata_res = resource_search(dataset_id, filename)
        if metadata_res:
            log.debug("Found existing resource {}".format(metadata_res))
            existing_id = metadata_res["id"]
        else:
            existing_id = None

        res = file_upload(
            dataset_id=dataset_id,
            filename=filename,
            filedata=meta_data,
            res_id=existing_id,
            format="json-ld",
            mime_type=mime_type,
            authorization=CSVTOCSVW_TOKEN,
        )

        # delete the datastore created from datapusher
        try:
            toolkit.get_action("datastore_delete")(
                {"ignore_auth": True}, {"id": csv_res["id"], "force": True}
            )
            log.debug("Datastore of resource {} deleted.".format(csv_res["id"]))
        except toolkit.ObjectNotFound:
            pass
        parse = CSVWtoRDF(meta_data, csv_data)
        if len(parse.tables) > 0:
            table_key = next(iter(parse.tables))
            table_data = parse.tables[table_key]
            headers = simple_columns(table_data["columns"])
            column_names = [column["id"] for column in headers]
            table_records = list()
            for line in table_data["lines"]:
                record = dict(zip(column_names, line[1:]))
                table_records.append(record)
            count = 0
            for i, chunk in enumerate(chunky(table_records, CHUNK_INSERT_ROWS)):
                records, is_it_the_last_chunk = chunk
                count += len(records)
                log.info(
                    "Saving chunk {number} {is_last}".format(
                        number=i, is_last="(last)" if is_it_the_last_chunk else ""
                    )
                )
                send_resource_to_datastore(
                    csv_res["id"], headers, records, s, is_it_the_last_chunk
                )
    else:
        log.debug("No data found.")
        errored = True
    if not errored:
        job_dict["status"] = "complete"
    else:
        job_dict["status"] = "errored"
    callback_csvtocsvw_hook(callback_url, api_key=CSVTOCSVW_TOKEN, job_dict=job_dict)
    return "error" if errored else None


def transform_csv(
    res_url, res_id, dataset_id, callback_url, last_updated, skip_if_no_changes=True
):
    # url = '{ckan}/dataset/{pkg}/resource/{res_id}/download/{filename}'.format(
    #         ckan=CKAN_URL, pkg=dataset_id, res_id=res_id, filename=res_url)
    context = {"session": model.meta.create_local_session(), "ignore_auth": True}
    CSVTOCSVW_TOKEN = toolkit.config.get("ckanext.csvtocsvw.ckan_token")
    SSL_VERIFY = toolkit.config.get("ckanext.csvtocsvw.ssl_verify")
    if not SSL_VERIFY:
        requests.packages.urllib3.disable_warnings()

    metadata = {
        "ckan_url": toolkit.config.get("ckan.site_url"),
        "resource_id": res_id,
        "task_created": last_updated,
        "original_url": res_url,
        "task_key": "csvtocsvw_transform",
    }
    job_info = dict()
    job_dict = dict(metadata=metadata, status="running", job_info=job_info)
    errored = False
    callback_csvtocsvw_hook(callback_url, api_key=CSVTOCSVW_TOKEN, job_dict=job_dict)
    metadata_res = toolkit.get_action("resource_show")(context, {"id": res_id})
    if metadata_res:
        filename, filedata, mime_type = csvw_to_rdf(
            res_url, format="turtle", authorization=CSVTOCSVW_TOKEN
        )
        # upload result to ckan
        rdf_res = resource_search(dataset_id, filename)
        if rdf_res:
            existing_id = rdf_res["id"]
            log.debug("Found existing resources {}".format(rdf_res))
        else:
            existing_id = None
        res = file_upload(
            dataset_id=dataset_id,
            filename=filename,
            filedata=filedata,
            res_id=existing_id,
            format=mime_type,
            mime_type=mime_type,
            authorization=CSVTOCSVW_TOKEN,
        )

    if not errored:
        job_dict["status"] = "complete"
    else:
        job_dict["status"] = "errored"
    callback_csvtocsvw_hook(callback_url, api_key=CSVTOCSVW_TOKEN, job_dict=job_dict)
    return "error" if errored else None


import itertools


def chunky(items, num_items_per_chunk):
    """
    Breaks up a list of items into chunks - multiple smaller lists of items.
    The last chunk is flagged up.

    :param items: Size of each chunks
    :type items: iterable
    :param num_items_per_chunk: Size of each chunks
    :type num_items_per_chunk: int

    :returns: multiple tuples: (chunk, is_it_the_last_chunk)
    :rtype: generator of (list, bool)
    """
    items_ = iter(items)
    chunk = list(itertools.islice(items_, num_items_per_chunk))
    while chunk:
        next_chunk = list(itertools.islice(items_, num_items_per_chunk))
        chunk_is_the_last_one = not next_chunk
        yield chunk, chunk_is_the_last_one
        chunk = next_chunk


class DatastoreEncoder(json.JSONEncoder):
    # Custon JSON encoder
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return str(obj)

        return json.JSONEncoder.default(self, obj)


def send_resource_to_datastore(
    resource_id, headers, records, session, is_it_the_last_chunk=True
):
    """
    Stores records in CKAN datastore
    """
    request = {
        "resource_id": resource_id,
        "fields": headers,
        "force": True,
        "records": records,
        "calculate_record_count": is_it_the_last_chunk,
    }
    # log.debug(request)
    ckan_url = toolkit.config.get("ckan.site_url")
    url = ckan_url + toolkit.url_for("api.action", logic_function="datastore_create")
    res = session.post(url, json=request)
    if res.status_code != 200:
        log.debug("Create of datastore for resource {} failed.".format(resource_id))
    else:
        log.debug("Datastore of resource {} created.".format(resource_id))


def get_resource(id):
    local_ckan = ckanapi.LocalCKAN()
    try:
        res = local_ckan.action.resource_show(id=id)
    except:
        return False
    else:
        return res


def resource_search(dataset_id, res_name):
    local_ckan = ckanapi.LocalCKAN()
    dataset = local_ckan.action.package_show(id=dataset_id)
    log.debug(dataset)
    for res in dataset["resources"]:
        if res["name"] == res_name:
            return res
    return None


def callback_csvtocsvw_hook(result_url, api_key, job_dict):
    """Tells CKAN about the result of the csvtocsvw (i.e. calls the callback
    function 'csvtocsvw_hook'). Usually called by the csvtocsvw queue job.
    """
    headers = {"Content-Type": "application/json"}
    if api_key:
        if ":" in api_key:
            header, key = api_key.split(":")
        else:
            header, key = "Authorization", api_key
        headers[header] = key

    try:
        result = requests.post(
            result_url,
            data=json.dumps(job_dict, cls=DatetimeJsonEncoder),
            verify=toolkit.config.get("ckanext.csvtocsvw.ssl_verify"),
            headers=headers,
        )
    except requests.ConnectionError:
        return False

    return result.status_code == requests.codes.ok


from io import BytesIO


def file_upload(
    dataset_id,
    filename,
    filedata,
    res_id=None,
    format="",
    group=None,
    mime_type="text/csv",
    authorization=None,
):
    data_stream = BytesIO(filedata)
    headers = {}
    if authorization:
        headers["Authorization"] = authorization
    if res_id:
        mp_encoder = MultipartEncoder(
            fields={"id": res_id, "upload": (filename, data_stream, mime_type)}
        )
    else:
        mp_encoder = MultipartEncoder(
            fields={
                "package_id": dataset_id,
                "name": filename,
                "format": format,
                "id": res_id,
                "upload": (filename, data_stream, mime_type),
            }
        )
    headers["Content-Type"] = mp_encoder.content_type
    ckan_url = toolkit.config.get("ckan.site_url")
    if res_id:
        url = ckan_url + toolkit.url_for("api.action", logic_function="resource_patch")
    else:
        url = ckan_url + toolkit.url_for("api.action", logic_function="resource_create")
    try:
        response = requests.post(
            url,
            headers=headers,
            data=mp_encoder,
            verify=toolkit.config.get("ckanext.csvtocsvw.ssl_verify"),
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        log.debug(f"HTTP error occurred: {http_err}")  # Handle HTTP errors
        log.debug(
            f"Status Code: {http_err.response.status_code}"
        )  # Optionally includes the status code
        log.debug(
            f"Response Body: {http_err.response.text}"
        )  # Optionally includes the response body
    except requests.exceptions.RequestException as req_err:
        log.debug(
            f"Request error occurred: {req_err}"
        )  # Handle other request-related errors
    except Exception as err:
        log.debug(f"An error occurred: {err}")  # Handle any other errors
    r = response.json()
    log.debug("file {} uploaded at: {}".format(filename, r))
    return r


def expand_url(base, url):
    p_url = urlparse(url)
    if not p_url.scheme in ["https", "http"]:
        # relative url?
        p_url = urljoin(base, p_url.path)
        return p_url
    else:
        return p_url.path.geturl()


class DatetimeJsonEncoder(json.JSONEncoder):
    # Custom JSON encoder
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
