import ckanext.datastore.logic.auth as auth
from ckan.logic.auth.create import resource_create

def csvtocsvw_annotate(context, data_dict):
    return auth.datastore_auth(context, data_dict)


def csvtocsvw_transform(context, data_dict):
    return resource_create(context, data_dict)


def get_auth_functions():
    return {
        "csvtocsvw_annotate": csvtocsvw_annotate,
        "csvtocsvw_transform": csvtocsvw_transform,
    }
