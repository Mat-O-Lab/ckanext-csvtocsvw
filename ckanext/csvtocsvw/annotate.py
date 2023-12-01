import json
import os
import re

import requests

csvtocsvw_url = os.environ.get("CKAN_CSVTOCSVW_URL")


def post_request(url, headers, data, files=None):
    try:
        if files:
            # should crate a multipart form upload
            response = requests.post(url, data=data, headers=headers, files=files)
        else:
            # a application json post request
            response = requests.post(url, data=json.dumps(data), headers=headers)
        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        # placeholder for save file / clean-up
        raise SystemExit(e) from None
    return response


def annotate_csv_uri(
    csv_url: str,
    encoding: str = "auto",
    authorization: str = ""
):
    # curl -X 'POST' \ 'https://csvtocsvw.matolab.org/api/annotation' \ -H 'accept: application/json' \ -H 'Content-Type: application/json' \ -d '{ "data_url": "https://github.com/Mat-O-Lab/CSVToCSVW/raw/main/examples/example.csv", "separator": "auto", "header_separator": "auto", "encoding": "auto" }'
    url = csvtocsvw_url + "/api/annotate"
    data = {"data_url": csv_url, "encoding": encoding}
    headers = {"Content-type": "application/json", "Accept": "application/json"}
    if authorization:
        headers['Authorization']=authorization
    r = post_request(url, headers, data).json()
    filename = r["filename"]
    file = json.dumps(r["filedata"], indent=4).encode("utf-8")
    print("csvw annotation file created, suggested name: {}".format(filename))
    return filename, file


def annotate_csv_upload(
    filepath: str,
    encoding: str = "auto",
):
    # curl -X 'POST' \ 'https://csvtocsvw.matolab.org/api/annotate_upload?encoding=auto' \ -H 'accept: application/json' \ -H 'Content-Type: multipart/form-data' \ -F 'file=@detection_runs.csv;type=text/csv'
    url = csvtocsvw_url + "/api/annotate_upload?encoding=auto"
    headers = {"accept": "application/json"}
    head, tail = os.path.split(filepath)
    files = {"file": (tail, open(filepath, "rb"), "text/csv")}
    response = requests.post(url, headers=headers, files=files)
    response.raise_for_status()
    return response.json()
    

def csvw_to_rdf(
    meta_url: str,
    format: str = "turtle",
    authorization = None    
):
    # curl -X 'POST' \ 'https://csvtocsvw.matolab.org/api/rdf' \ -H 'accept: application/json' \ -H 'Content-Type: application/json' \ -d '{ "metadata_url": "https://github.com/Mat-O-Lab/resources/raw/main/rdfconverter/tests/detection_runs-metadata.json", "format": "turtle" }'
    url = csvtocsvw_url + "/api/rdf"
    data = {"metadata_url": meta_url, "format": format}
    headers = {"Content-type": "application/json", "Accept": "application/json"}
    if authorization:
        headers['Authorization']=authorization
    # r = requests.post(url, data=json.dumps(data), headers=headers)
    r = post_request(url, headers, data)
    if r.status_code == 200:
        d = r.headers["content-disposition"]
        fname = re.findall("filename=(.+)", d)[0]
        print("got serialized table with name {}".format(fname))
        return fname, r.content
    else:
        return False
