import tempfile
import zipfile
import os
import hashlib
import math
import copy

import requests
import six
import ckanapi
import ckanapi.datapackage

from ckan import model
from ckan.plugins.toolkit import get_action, config
from ckanext.csvtocsvw.annotate import annotate_csv_upload
from ckanext.csvtocsvw.csvw_parser import CSVWtoRDF, simple_columns

import ckan.lib.helpers as h
import json
log = __import__('logging').getLogger(__name__)

CKAN_URL= os.environ.get("CKAN_SITE_URL","http://localhost:5000")
CSVTOCSVW_TOKEN= os.environ.get("CSVTOCSVW_TOKEN","")
CHUNK_INSERT_ROWS = 250


from werkzeug.datastructures import FileStorage as FlaskFileStorage

def update_resource_file(resource_id, f):
       context = {
           'ignore_auth': True,
           'user': '',
       }
       upload = cgi.FieldStorage()
       upload.filename = getattr(f, 'name', 'data')
       upload.file = f
       data = {
           'id': resource_id,
           'url': 'will-be-overwritten-automatically',
           'upload': upload,
       }
       return get_action('resource_update')(context, data)

from requests.auth import HTTPBasicAuth

def annotate_csv(res_url, res_id, dataset_id, skip_if_no_changes=True):
    # url = '{ckan}/dataset/{pkg}/resource/{res_id}/download/{filename}'.format(
    #         ckan=CKAN_URL, pkg=dataset_id, res_id=res_id, filename=res_url)
    csv_res=get_action('resource_show')({'ignore_auth': True}, {'id': res_id})
    log.debug('Annotating: {}'.format(csv_res['url']))
    #need to get it as string, casue url annotation doesnt work with private datasets
    #filename,filedata=annotate_csv_uri(csv_res['url'])

    s = requests.Session()
    s.headers.update({"Authorization": CSVTOCSVW_TOKEN})
    csv_data=s.get(csv_res['url']).content
    prefix,suffix=csv_res['url'].rsplit('/',1)[-1].rsplit('.',1)
    if not prefix:
        prefix='unnamed'
    if not suffix:
        suffix='csv'
    #log.debug(csv_data)
    with tempfile.NamedTemporaryFile(prefix=prefix,suffix='.'+suffix) as csv:
        csv.write(csv_data)
        csv.seek(0)
        csv_name=csv.name
        result=annotate_csv_upload(csv.name)
    meta_data=json.dumps(result['filedata'], indent=2)
    log.debug('Got result with name: {}'.format(result['filename']))
    #replace csv url and name
    csv_name=csv_name.rsplit('/',1)[-1]
    dummy_url="file:///src/{}/".format(csv_name)
    filename=prefix+'-metadata.json'
    log.debug('replacing url: {} with {}'.format(dummy_url,csv_res['url']))
    log.debug('replacing id: {} with {}'.format(csv_name,csv_res['name']))
    meta_data=meta_data.replace(dummy_url,csv_res['url']+'/').replace(csv_name,csv_res['name'])

    # Upload resource to CKAN as a new/updated resource
    #res=get_resource(res_id)
    metadata_res=resource_search(dataset_id,filename)
    #log.debug(meta_data)
    prefix,suffix=filename.rsplit('.',1)


    #f = tempfile.NamedTemporaryFile(prefix=prefix,suffix='.'+suffix,delete=False)
    with tempfile.NamedTemporaryFile(prefix=prefix,suffix='.'+suffix) as metadata_file:
        metadata_file.write(meta_data.encode('utf-8'))
        metadata_file.seek(0)
        temp_file_name = metadata_file.name
        upload=FlaskFileStorage(open(temp_file_name, 'rb'), filename)
        resource = dict(
            package_id=dataset_id,
            #url='dummy-value',
            upload=upload,
            name=filename,
            format=u'json-ld'
        )
        if not metadata_res:
            log.debug('Writing new resource to - {}'.format(dataset_id))
            #local_ckan.action.resource_create(**resource)
            metadata_res=get_action('resource_create')({'ignore_auth': True}, resource)
            log.debug(metadata_res)
            
        else:
            log.debug('Updating resource - {}'.format(metadata_res['id']))
            # local_ckan.action.resource_patch(
            #     id=res['id'],
            #     **resource)
            resource['id']= metadata_res['id']
            get_action('resource_update')({'ignore_auth': True}, resource)
    #delete the datastore created from datapusher
    delete_datastore_resource(csv_res['id'],s)
    #use csvw metadata to readout the cvs
    parse=CSVWtoRDF(meta_data, csv_data)
    #pick table one, can only put one table to datastore
    table_key=next(iter(parse.tables))
    table_data=parse.tables[table_key]
    headers=simple_columns(table_data['columns'])
    log.debug(headers)
    # headers=[
    #     {
    #         "id":  "value",
    #         "type":   "numeric",
    #         "info": {
    #             "label":  "Value",
    #             "notes":  "An xample Value Column",
    #             #"type_override":  # type for datapusher to use when importing data
    #             #...:  # other user-defined fields
    #         }
    #     }
    # ]
    # records=[
    # {
    #     "index": 1,
    #     "value": 2
    # },
    # {
    #     "index": 2,
    #     "value": 3
    # }
    # ]
    column_names=[column['id'] for column in headers]
    table_records=list()
    for line in table_data['lines']:
        record=dict()
        for i, value in enumerate(line[1:]):
            record[column_names[i]]=value
        table_records.append(record)
    #log.debug(table_records[:3])
    count = 0
    for i, chunk in enumerate(chunky(table_records, CHUNK_INSERT_ROWS)):
        records, is_it_the_last_chunk = chunk
        count += len(records)
        log.info('Saving chunk {number} {is_last}'.format(
            number=i, is_last='(last)' if is_it_the_last_chunk else ''))
        send_resource_to_datastore(csv_res['id'],headers,records,s, is_it_the_last_chunk)

        

    # datapackage, ckan_and_datapackage_resources, existing_zip_resource = \
    #     generate_datapackage_json(package_id)

    # if skip_if_no_changes and existing_zip_resource and \
    #         not has_datapackage_changed_significantly(
    #             datapackage, ckan_and_datapackage_resources,
    #             existing_zip_resource):
    #     log.info('Skipping updating the zip - the datapackage.json is not '
    #              'changed sufficiently: {}'.format(dataset['name']))
    #     return

    # prefix = "{}-".format(dataset[u'name'])
    # with tempfile.NamedTemporaryFile(prefix=prefix, suffix='.zip') as fp:
    #     write_zip(fp, datapackage, ckan_and_datapackage_resources)

    #     # Upload resource to CKAN as a new/updated resource
    #     local_ckan = ckanapi.LocalCKAN()
    #     fp.seek(0)
    #     resource = dict(
    #         package_id=dataset['id'],
    #         url='dummy-value',
    #         upload=fp,
    #         name=u'All resource data',
    #         format=u'ZIP',
    #         csvtocsvw_metadata_modified=dataset['metadata_modified'],
    #         csvtocsvw_datapackage_hash=hash_datapackage(datapackage)
    #     )

    #     if not existing_zip_resource:
    #         log.debug('Writing new zip resource - {}'.format(dataset['name']))
    #         local_ckan.action.resource_create(**resource)
    #     else:
    #         # TODO update the existing zip resource (using patch?)
    #         log.debug('Updating zip resource - {}'.format(dataset['name']))
    #         local_ckan.action.resource_patch(
    #             id=existing_zip_resource['id'],
    #             **resource)

try:
    from urllib.parse import urlsplit
except ImportError:
    from urlparse import urlsplit

def get_url(action):
    """
    Get url for ckan action
    """
    if not urlsplit(CKAN_URL).scheme:
        ckan_url = 'http://' + CKAN_URL.lstrip('/')
    ckan_url = CKAN_URL.rstrip('/')
    return '{ckan_url}/api/3/action/{action}'.format(
        ckan_url=ckan_url, action=action)

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
    delete_url = get_url('datastore_delete')
    res = session.post(
        delete_url, 
        json={'id': resource_id, 'force': True}
        )
    if res.status_code!=200:
        log.debug(res.content)
        log.debug('Deleting existing datastore of resource {} failed.'.format(resource_id))
    else:
        log.debug('Datastore of resource {} deleted.'.format(resource_id))

class DatastoreEncoder(json.JSONEncoder):
    # Custon JSON encoder
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return str(obj)

        return json.JSONEncoder.default(self, obj)

def send_resource_to_datastore(resource_id, headers, records, session, is_it_the_last_chunk=True):
    """
    Stores records in CKAN datastore
    """
    request = {'resource_id': resource_id,
               'fields': headers,
               'force': True,
               'records': records,
               'calculate_record_count': is_it_the_last_chunk
               }
    #log.debug(request)
    url = get_url('datastore_create')
    res = session.post(url, json=request)
    if res.status_code!=200:
        log.debug(res.content)
        log.debug('Create of datastore for resource {} failed.'.format(resource_id))
    else:
        log.debug('Datastore of resource {} created.'.format(resource_id))


def get_resource(id):
    local_ckan = ckanapi.LocalCKAN()
    try:
        res=local_ckan.action.resource_show(id=id)
    except:
        return False
    else:
        return res

def resource_search(dataset_id, res_name):
    local_ckan = ckanapi.LocalCKAN()
    dataset=local_ckan.action.package_show(id=dataset_id)
    log.debug(dataset)
    for res in dataset['resources']:
        if res['name']==res_name:
            return res
    return None

def update_zip(package_id, skip_if_no_changes=True):
    '''
    Create/update the a dataset's zip resource, containing the other resources
    and some metadata.

    :param skip_if_no_changes: If true, and there is an existing zip for this
        dataset, it will compare a freshly generated package.json against what
        is in the existing zip, and if there are no changes (ignoring the
        Download All Zip) then it will skip downloading the resources and
        updating the zip.
    '''
    # TODO deal with private datasets - 'ignore_auth': True
    context = {'model': model, 'session': model.Session}
    dataset = get_action('package_show')(context, {'id': package_id})
    log.debug('Updating zip: {}'.format(dataset['name']))

    datapackage, ckan_and_datapackage_resources, existing_zip_resource = \
        generate_datapackage_json(package_id)

    if skip_if_no_changes and existing_zip_resource and \
            not has_datapackage_changed_significantly(
                datapackage, ckan_and_datapackage_resources,
                existing_zip_resource):
        log.info('Skipping updating the zip - the datapackage.json is not '
                 'changed sufficiently: {}'.format(dataset['name']))
        return

    prefix = "{}-".format(dataset[u'name'])
    with tempfile.NamedTemporaryFile(prefix=prefix, suffix='.zip') as fp:
        write_zip(fp, datapackage, ckan_and_datapackage_resources)

        # Upload resource to CKAN as a new/updated resource
        local_ckan = ckanapi.LocalCKAN()
        fp.seek(0)
        resource = dict(
            package_id=dataset['id'],
            url='dummy-value',
            upload=fp,
            name=u'All resource data',
            format=u'ZIP',
            csvtocsvw_metadata_modified=dataset['metadata_modified'],
            csvtocsvw_datapackage_hash=hash_datapackage(datapackage)
        )

        if not existing_zip_resource:
            log.debug('Writing new zip resource - {}'.format(dataset['name']))
            local_ckan.action.resource_create(**resource)
        else:
            # TODO update the existing zip resource (using patch?)
            log.debug('Updating zip resource - {}'.format(dataset['name']))
            local_ckan.action.resource_patch(
                id=existing_zip_resource['id'],
                **resource)


class DownloadError(Exception):
    pass


def has_datapackage_changed_significantly(
        datapackage, ckan_and_datapackage_resources, existing_zip_resource):
    '''Compare the freshly generated datapackage with the existing one and work
    out if it is changed enough to warrant regenerating the zip.

    :returns bool: True if the data package has really changed and needs
        regenerating
    '''
    assert existing_zip_resource
    new_hash = hash_datapackage(datapackage)
    old_hash = existing_zip_resource.get('csvtocsvw_datapackage_hash')
    return new_hash != old_hash


def hash_datapackage(datapackage):
    '''Returns a hash of the canonized version of the given datapackage
    (metadata).
    '''
    canonized = canonized_datapackage(datapackage)
    m = hashlib.md5(six.text_type(make_hashable(canonized)).encode('utf8'))
    return m.hexdigest()


def make_hashable(obj):
    if isinstance(obj, (tuple, list)):
        return tuple((make_hashable(e) for e in obj))
    if isinstance(obj, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in obj.items()))
    return obj


def canonized_datapackage(datapackage):
    '''
    The given datapackage is 'canonized', so that an exsting one can be
    compared with a freshly generated one, to see if the zip needs
    regenerating.

    Datapackages resources have either:
    * local paths (downloaded into the package) OR
    * OR remote paths (URLs)
    To allow datapackages to be compared, the canonization converts local
    resources to remote ones.
    '''
    datapackage_ = copy.deepcopy(datapackage)
    # convert resources to remote paths
    # i.e.
    #
    #   "path": "annual-.csv", "sources": [
    #     {
    #       "path": "https://example.com/file.csv",
    #       "title": "annual.csv"
    #     }
    #   ],
    #
    # ->
    #
    #   "path": "https://example.com/file.csv",
    for res in datapackage_.get('resources', []):
        try:
            remote_path = res['sources'][0]['path']
        except KeyError:
            continue
        res['path'] = remote_path
        del res['sources']
    return datapackage_


def generate_datapackage_json(package_id):
    '''Generates the datapackage - metadata that would be saved as
    datapackage.json.
    '''
    context = {'model': model, 'session': model.Session}
    dataset = get_action('package_show')(
        context, {'id': package_id})

    # filter out resources that are not suitable for inclusion in the data
    # package
    local_ckan = ckanapi.LocalCKAN()
    dataset, resources_to_include, existing_zip_resource = \
        remove_resources_that_should_not_be_included_in_the_datapackage(
            dataset)

    # get the datapackage (metadata)
    datapackage = ckanapi.datapackage.dataset_to_datapackage(dataset)

    # populate datapackage with the schema from the Datastore data
    # dictionary
    ckan_and_datapackage_resources = zip(resources_to_include,
                                         datapackage.get('resources', []))
    for res, datapackage_res in ckan_and_datapackage_resources:
        ckanapi.datapackage.populate_datastore_res_fields(
            ckan=local_ckan, res=res)
        ckanapi.datapackage.populate_schema_from_datastore(
            cres=res, dres=datapackage_res)

    # add in any other dataset fields, if configured
    fields_to_include = config.get(
        u'ckanext.downloadall.dataset_fields_to_add_to_datapackage',
        u'').split()
    for key in fields_to_include:
        datapackage[key] = dataset.get(key)

    return (datapackage, ckan_and_datapackage_resources,
            existing_zip_resource)


def write_zip(fp, datapackage, ckan_and_datapackage_resources):
    '''
    Downloads resources and writes the zip file.

    :param fp: Open file that the zip can be written to
    '''
    with zipfile.ZipFile(fp, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) \
            as zipf:
        i = 0
        for res, dres in ckan_and_datapackage_resources:
            i += 1

            log.debug('Downloading resource {}/{}: {}'
                      .format(i, len(ckan_and_datapackage_resources),
                              res['url']))
            filename = \
                ckanapi.datapackage.resource_filename(dres)
            try:
                download_resource_into_zip(res['url'], filename, zipf)
            except DownloadError:
                # The dres['path'] is left as the url - i.e. an 'external
                # resource' of the data package.
                continue

            save_local_path_in_datapackage_resource(dres, res, filename)

            # TODO optimize using the file_hash

        # Add the datapackage.json
        write_datapackage_json(datapackage, zipf)

    statinfo = os.stat(fp.name)
    filesize = statinfo.st_size

    log.info('Zip created: {} {} bytes'.format(fp.name, filesize))

    return filesize


def save_local_path_in_datapackage_resource(datapackage_resource, res,
                                            filename):
    # save path in datapackage.json - i.e. now pointing at the file
    # bundled in the data package zip
    title = datapackage_resource.get('title') or res.get('title') \
        or res.get('name', '')
    datapackage_resource['sources'] = [
        {'title': title, 'path': datapackage_resource['path']}]
    datapackage_resource['path'] = filename


def download_resource_into_zip(url, filename, zipf):
    try:
        r = requests.get(url, stream=True)
        r.raise_for_status()
    except requests.ConnectionError:
        log.error('URL {url} refused connection. The resource will not'
                  ' be downloaded'.format(url=url))
        raise DownloadError()
    except requests.exceptions.HTTPError as e:
        log.error('URL {url} status error: {status}. The resource will'
                  ' not be downloaded'
                  .format(url=url, status=e.response.status_code))
        raise DownloadError()
    except requests.exceptions.RequestException as e:
        log.error('URL {url} download request exception: {error}'
                  .format(url=url, error=str(e)))
        raise DownloadError()
    except Exception as e:
        log.error('URL {url} download exception: {error}'
                  .format(url=url, error=str(e)))
        raise DownloadError()

    hash_object = hashlib.md5()
    size = 0
    try:
        # python3 syntax - stream straight into the zip
        with zipf.open(filename, 'wb') as zf:
            for chunk in r.iter_content(chunk_size=128):
                zf.write(chunk)
                hash_object.update(chunk)
                size += len(chunk)
    except RuntimeError:
        # python2 syntax - need to save to disk first
        with tempfile.NamedTemporaryFile() as datafile:
            for chunk in r.iter_content(chunk_size=128):
                datafile.write(chunk)
                hash_object.update(chunk)
                size += len(chunk)
            datafile.flush()
            # .write() streams the file into the zip
            zipf.write(datafile.name, arcname=filename)
    file_hash = hash_object.hexdigest()
    log.debug('Downloaded {}, hash: {}'
              .format(format_bytes(size), file_hash))


def write_datapackage_json(datapackage, zipf):
    with tempfile.NamedTemporaryFile() as json_file:
        json_file.write(ckanapi.cli.utils.pretty_json(datapackage))
        json_file.flush()
        zipf.write(json_file.name, arcname='datapackage.json')
        log.debug('Added datapackage.json from {}'.format(json_file.name))


def format_bytes(size_bytes):
    if size_bytes == 0:
        return "0 bytes"
    size_name = ("bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 1)
    return '{} {}'.format(s, size_name[i])


def remove_resources_that_should_not_be_included_in_the_datapackage(dataset):
    resource_formats_to_ignore = ['API', 'api']  # TODO make it configurable

    existing_zip_resource = None
    resources_to_include = []
    for i, res in enumerate(dataset['resources']):
        if res.get('csvtocsvw_metadata_modified'):
            # this is an existing zip of all the other resources
            log.debug('Resource resource {}/{} skipped - is the zip itself'
                      .format(i + 1, len(dataset['resources'])))
            existing_zip_resource = res
            continue

        if res['format'] in resource_formats_to_ignore:
            log.debug('Resource resource {}/{} skipped - because it is '
                      'format {}'.format(i + 1, len(dataset['resources']),
                                         res['format']))
            continue
        resources_to_include.append(res)
    dataset = dict(dataset, resources=resources_to_include)
    return dataset, resources_to_include, existing_zip_resource