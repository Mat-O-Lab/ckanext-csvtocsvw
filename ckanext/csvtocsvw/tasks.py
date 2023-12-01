import json
import os
import tempfile

import ckanapi
import ckanapi.datapackage
import requests
from ckan import model
from ckan.plugins.toolkit import get_action, asbool
from ckanext.csvtocsvw.annotate import annotate_csv_upload, csvw_to_rdf, annotate_csv_uri
from ckanext.csvtocsvw.csvw_parser import CSVWtoRDF, simple_columns
import datetime
from urllib.parse import urlparse, urljoin, urlsplit
from requests_toolbelt.multipart.encoder import MultipartEncoder


log = __import__("logging").getLogger(__name__)

CKAN_URL = os.environ.get("CKAN_SITE_URL", "http://localhost:5000")
CSVTOCSVW_TOKEN = os.environ.get("CSVTOCSVW_TOKEN", "")
CHUNK_INSERT_ROWS = 250

SSL_VERIFY = asbool(os.environ.get("CSVTOCSVW_SSL_VERIFY", True))
if not SSL_VERIFY:
    requests.packages.urllib3.disable_warnings()


from werkzeug.datastructures import FileStorage as FlaskFileStorage

def annotate_csv(res_url, res_id, dataset_id, callback_url, last_updated, skip_if_no_changes=True):
    # url = '{ckan}/dataset/{pkg}/resource/{res_id}/download/{filename}'.format(
    #         ckan=CKAN_URL, pkg=dataset_id, res_id=res_id, filename=res_url)
    context={
        'session': model.meta.create_local_session(),
        "ignore_auth": True
        }
    metadata = {
            'ckan_url': CKAN_URL,
            'resource_id': res_id,
            'task_created': last_updated,
            'original_url': res_url,
            'task_key': "csvtocsvw_annotate"
        }
    job_info=dict()
    job_dict = dict(metadata=metadata,
                    status='running',
                    job_info=job_info
    )
    errored = False
    callback_csvtocsvw_hook(callback_url,
                          api_key=CSVTOCSVW_TOKEN,
                          job_dict=job_dict)

    csv_res = get_action("resource_show")(context, {"id": res_id})
    log.debug("Annotating: {}".format(csv_res["url"]))
    
    s = requests.Session()
    s.headers.update({"Authorization": CSVTOCSVW_TOKEN})
    csv_data = s.get(csv_res["url"]).content
    #prefix, suffix = csv_res["url"].rsplit("/", 1)[-1].rsplit(".", 1)
    filename,meta_data = annotate_csv_uri(csv_res["url"],authorization=CSVTOCSVW_TOKEN)
    if meta_data:
        # Upload resource to CKAN as a new/updated resource
        # res=get_resource(res_id)
        metadata_res = resource_search(dataset_id, filename)
        # log.debug(meta_data)
        prefix, suffix = filename.rsplit(".", 1)
        if metadata_res:
            log.debug("Found existing resource {}".format(metadata_res))
            existing_id=metadata_res['id']
        else:
            existing_id=None
        
        res=file_upload(dataset_id=dataset_id, filename=filename, filedata=meta_data,res_id=existing_id, format='json-ld', authorization=CSVTOCSVW_TOKEN)
    
        # delete the datastore created from datapusher
        delete_datastore_resource(csv_res["id"], s)
        # use csvw metadata to readout the cvs
        parse = CSVWtoRDF(meta_data, csv_data)
        # pick table one, can only put one table to datastore
        if len(parse.tables)>0:
            table_key = next(iter(parse.tables))
            table_data = parse.tables[table_key]
            headers = simple_columns(table_data["columns"])
            log.debug(headers)

            column_names = [column["id"] for column in headers]
            table_records = list()
            for line in table_data["lines"]:
                record = dict()
                for i, value in enumerate(line[1:]):
                    record[column_names[i]] = value
                table_records.append(record)
            # log.debug(table_records[:3])
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
    if not errored:
        job_dict['status'] = 'complete'
    else:
        job_dict['status'] = 'errored'
    callback_csvtocsvw_hook(callback_url,
                          api_key=CSVTOCSVW_TOKEN,
                          job_dict=job_dict)
    return 'error' if errored else None


def transform_csv(res_url, res_id, dataset_id, callback_url, last_updated, skip_if_no_changes=True):
    # url = '{ckan}/dataset/{pkg}/resource/{res_id}/download/{filename}'.format(
    #         ckan=CKAN_URL, pkg=dataset_id, res_id=res_id, filename=res_url)
    context={
        'session': model.meta.create_local_session(),
        "ignore_auth": True
        }
    metadata = {
            'ckan_url': CKAN_URL,
            'resource_id': res_id,
            'task_created': last_updated,
            'original_url': res_url,
            'task_key': "csvtocsvw_transform"
        }
    job_info=dict()
    job_dict = dict(metadata=metadata,
                    status='running',
                    job_info=job_info
    )
    errored = False
    callback_csvtocsvw_hook(callback_url,
                          api_key=CSVTOCSVW_TOKEN,
                          job_dict=job_dict)
    csv_res = get_action("resource_show")(context, {"id": res_id})
    # need to get it as string, casue url annotation doesnt work with private datasets
    # filename,filedata=annotate_csv_uri(csv_res['url'])
    prefix, suffix = csv_res["url"].rsplit("/", 1)[-1].rsplit(".", 1)
    if not prefix:
        prefix = "unnamed"
    format="turtle"
    metadata_res = resource_search(dataset_id, prefix+"-metadata.json")
    log.debug("Transforming {} with metedata {}".format(csv_res["url"],metadata_res['url']))
    filename,filedata=csvw_to_rdf(metadata_res['url'],format=format,authorization=CSVTOCSVW_TOKEN)
    #upload result to ckan
    rdf_res = resource_search(dataset_id, filename)
    if rdf_res:
        existing_id=rdf_res['id']
        log.debug("Found existing resources {}".format(rdf_res))
    else:
        existing_id=None
    res=file_upload(dataset_id=dataset_id, filename=filename, filedata=filedata,res_id=existing_id, format=format, authorization=CSVTOCSVW_TOKEN)
        
    if not errored:
        job_dict['status'] = 'complete'
    else:
        job_dict['status'] = 'errored'
    callback_csvtocsvw_hook(callback_url,
                          api_key=CSVTOCSVW_TOKEN,
                          job_dict=job_dict)
    return 'error' if errored else None


def get_url(action):
    """
    Get url for ckan action
    """
    if not urlsplit(CKAN_URL).scheme:
        ckan_url = "http://" + CKAN_URL.lstrip("/")
    ckan_url = CKAN_URL.rstrip("/")
    return "{ckan_url}/api/3/action/{action}".format(ckan_url=ckan_url, action=action)


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


def delete_datastore_resource(resource_id, session):
    delete_url = get_url("datastore_delete")
    res = session.post(delete_url, json={"id": resource_id, "force": True})
    if res.status_code != 200:
        log.debug(res.content)
        log.debug(
            "Deleting existing datastore of resource {} failed.".format(resource_id)
        )
    else:
        log.debug("Datastore of resource {} deleted.".format(resource_id))


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
    url = get_url("datastore_create")
    res = session.post(url, json=request)
    if res.status_code != 200:
        log.debug(res.content)
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
    '''Tells CKAN about the result of the csvtocsvw (i.e. calls the callback
    function 'csvtocsvw_hook'). Usually called by the csvtocsvw queue job.
    '''
    headers = {'Content-Type': 'application/json'}
    if api_key:
        if ':' in api_key:
            header, key = api_key.split(':')
        else:
            header, key = 'Authorization', api_key
        headers[header] = key

    try:
        result = requests.post(
            result_url,
            data=json.dumps(job_dict, cls=DatetimeJsonEncoder),
            verify=SSL_VERIFY,
            headers=headers)
    except requests.ConnectionError:
        return False

    return result.status_code == requests.codes.ok

from io import BytesIO

def file_upload(dataset_id, filename, filedata, res_id=None,format='', group=None, mime_type='text/csv', authorization=None):
    data_stream=BytesIO(filedata)
    headers={}
    if authorization:
        headers['Authorization']=authorization
    if res_id:
        mp_encoder = MultipartEncoder(
        fields={
            'id': res_id,
            'upload': (filename, data_stream, mime_type)
        }
        )
    else:
        mp_encoder = MultipartEncoder(
        fields={
            "package_id": dataset_id,
            "name": filename,
            "format": format,
            "id": res_id,
            "upload": (filename, data_stream, mime_type)
        }
        )
    headers['Content-Type']= mp_encoder.content_type
    if res_id:
        url=expand_url(CKAN_URL,'/api/action/resource_patch')
    else:
        url=expand_url(CKAN_URL,'/api/action/resource_create')
    response=requests.post(url, headers=headers, data=mp_encoder)
    response.raise_for_status()
    r=response.json()
    log.debug('file {} uploaded at: {}'.format(filename,r))
    return r

def expand_url(base, url):
    p_url = urlparse(url)
    if not p_url.scheme in ['https', 'http']:
        #relative url?
        p_url=urljoin(base, p_url.path)
        return p_url
    else:
        return p_url.path.geturl()


class DatetimeJsonEncoder(json.JSONEncoder):
    # Custom JSON encoder
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()

