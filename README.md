[![Tests](https://github.com/Mat-O-Lab/ckanext-csvtocsvw/actions/workflows/test.yml/badge.svg)](https://github.com/Mat-O-Lab/ckanext-csvtocsvw/actions/workflows/test.yml)

# ckanext-csvtocsvw

Extension automatically generating csvw metadata for uploaded textual tabular data. It uploads the data of the first table documented into a datastore for the source csv file.
**should be used as replacement for datapusher**

## Requirements
Needs a running instance of the [CSVToCSVW Application](https://github.com/Mat-O-Lab/CSVToCSVW). 
Point at it through env variables.
Also needed is a Api Token for an account with the right privaledges to make the background job work on private datasets and ressources.

```bash
CKANINI__CKANEXT__CSVTOCSVW_URL=http://${CSVTOCSVW_HOST}:${CSVTOCSVW_APP_PORT}
CKANINI__CKANEXT__CSVTOCSVW__CKAN_TOKEN=${CKAN_API_TOKEN}
```

You can set the default formats to annotate by setting the env variable CSVTOCSVW_FORMATS for example
```bash
CKANINI__CKANEXT__CSVTOCSVW__FORMATS="csv txt asc"
```
else it will react to the following  formats: "csv", "txt", "asc", "tsv"

If you need to process files that are not hosted through https (CKAN is not ssl configured), you can disable ssl verification
```bash
CKANINI__CKANEXT__CSVTOCSVW__SSL_VERIFY=False
```

## Purpose
Reacts to CSV files uploaded. DEFAULT_FORMATS are "csv; txt" It creates two to sites for each resource.
- /annotate creates CSVW annotation file for a CSV in json-ld format named <csv_filename>-metadata.json, uploades table-1 to ckan datastore o u can explorer it with recline views
- /transform utilizes CSVW metadata to transform the whole content of the csv file to rdf, output is <csv_filename>.ttl
The plugins default behavior includes a trigger to csv file uploads, so it runs annotation automatically on upload.
The transformation is a bonus feature and outputs standard tabular data as mentioned in the CSVW documentation of the W3C. It must be triggered manually.

Compatibility with core CKAN versions:

| CKAN version    | Compatible?   |
| --------------- | ------------- |
| 2.9 and arlier  | not tested    |
| 2.10             | yes    |
| 2.11            | yes    |

* "yes"
* "not tested" - I can't think of a reason why it wouldn't work
* "not yet" - there is an intention to get it working
* "no"


## Installation

To install the extension:

1. Activate your CKAN virtual environment, for example:
```bash
. /usr/lib/ckan/default/bin/activate
```
2. Use pip to install package
```bash
pip install ckanext-csvtocsvw
```
3. Add `csvtocsvw` to the `ckan.plugins` setting in your CKAN
   config file (by default the config file is located at
   `/etc/ckan/default/ckan.ini`).

4. Restart CKAN. For example, if you've deployed CKAN with Apache on Ubuntu:
```bash
sudo service apache2 reload
```

## Developer installation

To install ckanext-csvtocsvw for development, activate your CKAN virtualenv and
do:
```bash
git clone https://github.com/Mat-O-Lab/ckanext-csvtocsvw.git
cd ckanext-csvtocsvw
python setup.py develop
pip install -r dev-requirements.txt
```

## Tests

To run the tests, do:
```bash
pytest --ckan-ini=test.ini
```

## License

[AGPL](https://www.gnu.org/licenses/agpl-3.0.en.html)

# Acknowledgments
The authors would like to thank the Federal Government and the Heads of Government of the Länder for their funding and support within the framework of the [Platform Material Digital](https://www.materialdigital.de) consortium. Funded by the German [Federal Ministry of Education and Research (BMBF)](https://www.bmbf.de/bmbf/en/) through the [MaterialDigital](https://www.bmbf.de/SharedDocs/Publikationen/de/bmbf/5/31701_MaterialDigital.pdf?__blob=publicationFile&v=5) Call in Project [KupferDigital](https://www.materialdigital.de/project/1) - project id 13XP5119.

