[![Tests](https://github.com/Mat-O-Lab/ckanext-csvtocsvw/workflows/Tests/badge.svg?branch=main)](https://github.com/Mat-O-Lab/ckanext-csvtocsvw/actions)

# ckanext-csvtocsvw

Extension automatically generating csvw metadata for uploaded textual tabular data. It uploads the data of the first table documented into a datastore for the source csv file.
**should be used as replacement for datapusher**

## Requirements
Needs a running instance of the [CSVToCSVW Application](https://github.com/Mat-O-Lab/CSVToCSVW). 
Point at it through env variables.
Also needed is a Api Token for an account with the right privaledges to make the background job work on private datasets and ressources.

´´´cmd
CKAN_CSVTOCSVW_URL=http://${CSVTOCSVW_HOST}:${CSVTOCSVW_APP_PORT}
CSVTOCSVW_TOKEN=${CKAN_DATAPUSHER_URL_API_TOKEN}
´´´

**TODO:** For example, you might want to mention here which versions of CKAN this
extension works with.

If your extension works across different versions you can add the following table:

Compatibility with core CKAN versions:

| CKAN version    | Compatible?   |
| --------------- | ------------- |
| 2.8 and arlier  | not tested    |
| 2.9             | yes    |
| 2.10            | yes    |

Suggested values:

* "yes"
* "not tested" - I can't think of a reason why it wouldn't work
* "not yet" - there is an intention to get it working
* "no"


## Installation

**TODO:** Add any additional install steps to the list below.
   For example installing any non-Python dependencies or adding any required
   config settings.

To install ckanext-csvtocsvw:

1. Activate your CKAN virtual environment, for example:

     . /usr/lib/ckan/default/bin/activate

2. Clone the source and install it on the virtualenv

    git clone https://github.com/Mat-O-Lab/ckanext-csvtocsvw.git
    cd ckanext-csvtocsvw
    pip install -e .
	pip install -r requirements.txt

3. Add `csvtocsvw` to the `ckan.plugins` setting in your CKAN
   config file (by default the config file is located at
   `/etc/ckan/default/ckan.ini`).

4. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu:

     sudo service apache2 reload


## Config settings

None at present

**TODO:** Document any optional config settings here. For example:

	# The minimum number of hours to wait before re-checking a resource
	# (optional, default: 24).
	ckanext.csvtocsvw.some_setting = some_default_value


## Developer installation

To install ckanext-csvtocsvw for development, activate your CKAN virtualenv and
do:

    git clone https://github.com/Mat-O-Lab/ckanext-csvtocsvw.git
    cd ckanext-csvtocsvw
    python setup.py develop
    pip install -r dev-requirements.txt


## Tests

To run the tests, do:

    pytest --ckan-ini=test.ini


## Releasing a new version of ckanext-csvtocsvw

If ckanext-csvtocsvw should be available on PyPI you can follow these steps to publish a new version:

1. Update the version number in the `setup.py` file. See [PEP 440](http://legacy.python.org/dev/peps/pep-0440/#public-version-identifiers) for how to choose version numbers.

2. Make sure you have the latest version of necessary packages:

    pip install --upgrade setuptools wheel twine

3. Create a source and binary distributions of the new version:

       python setup.py sdist bdist_wheel && twine check dist/*

   Fix any errors you get.

4. Upload the source distribution to PyPI:

       twine upload dist/*

5. Commit any outstanding changes:

       git commit -a
       git push

6. Tag the new release of the project on GitHub with the version number from
   the `setup.py` file. For example if the version number in `setup.py` is
   0.0.1 then do:

       git tag 0.0.1
       git push --tags

## License

[AGPL](https://www.gnu.org/licenses/agpl-3.0.en.html)
