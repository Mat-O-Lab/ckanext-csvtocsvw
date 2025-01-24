import os
import re

import ckan.plugins.toolkit as toolkit
import requests


def annotate_csv_uri(csv_url: str, encoding: str = "auto", authorization: str = ""):
    csvtocsvw_url = toolkit.config.get("ckanext.csvtocsvw.csvtocsvw_url")
    # curl -X 'POST' \ 'https://csvtocsvw.matolab.org/api/annotation' \ -H 'accept: application/json' \ -H 'Content-Type: application/json' \ -d '{ "data_url": "https://github.com/Mat-O-Lab/CSVToCSVW/raw/main/examples/example.csv", "separator": "auto", "header_separator": "auto", "encoding": "auto" }'
    url = csvtocsvw_url + "/api/annotate?return_type=json-ld"
    data = {"data_url": csv_url, "encoding": encoding}
    headers = {"Content-type": "application/json", "Accept": "accept: */*"}
    if authorization:
        headers["Authorization"] = authorization
    r = requests.post(url, headers=headers, json=data)
    r.raise_for_status()
    if r.status_code == 200:
        d = r.headers["content-disposition"]
        mime_type = r.headers["content-type"]
        filename = re.findall("filename=(.+)", d)[0]
        file = r.content
        print("csvw annotation file created, suggested name: {}".format(filename))
        return filename, file, mime_type


def annotate_csv_upload(
    filepath: str,
    encoding: str = "auto",
):
    csvtocsvw_url = toolkit.config.get("ckanext.csvtocsvw.csvtocsvw_url")
    # curl -X 'POST' \ 'https://csvtocsvw.matolab.org/api/annotate_upload?encoding=auto' \ -H 'accept: application/json' \ -H 'Content-Type: multipart/form-data' \ -F 'file=@detection_runs.csv;type=text/csv'
    url = csvtocsvw_url + "/api/annotate_upload?encoding=auto&return_type=json-ld"
    headers = {"accept": "application/json"}
    head, tail = os.path.split(filepath)
    files = {"file": (tail, open(filepath, "rb"), "text/csv")}
    r = requests.post(url, headers=headers, files=files)
    r.raise_for_status()
    if r.status_code == 200:
        d = r.headers["content-disposition"]
        mime_type = r.headers["content-type"]
        filename = re.findall("filename=(.+)", d)[0]
        file = r.content
        print("csvw annotation file created, suggested name: {}".format(filename))
        return filename, file, mime_type


def csvw_to_rdf(meta_url: str, format: str = "turtle", authorization=None):
    csvtocsvw_url = toolkit.config.get("ckanext.csvtocsvw.csvtocsvw_url")
    # curl -X 'POST' \ 'https://csvtocsvw.matolab.org/api/rdf' \ -H 'accept: application/json' \ -H 'Content-Type: application/json' \ -d '{ "metadata_url": "https://github.com/Mat-O-Lab/resources/raw/main/rdfconverter/tests/detection_runs-metadata.json", "format": "turtle" }'
    url = csvtocsvw_url + "/api/rdf?return_type=" + format
    data = {"metadata_url": meta_url, "format": format}
    headers = {"Content-type": "application/json", "Accept": "application/json"}
    if authorization:
        headers["Authorization"] = authorization
    # r = requests.post(url, data=json.dumps(data), headers=headers)
    r = requests.post(url, headers=headers, json=data)
    r.raise_for_status()
    if r.status_code == 200:
        d = r.headers["content-disposition"]
        mime_type = r.headers["content-type"]
        fname = re.findall("filename=(.+)", d)[0]
        print("got serialized table with name {}".format(fname))
        return fname, r.content, mime_type
