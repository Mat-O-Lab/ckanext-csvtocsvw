import ckanext.datastore.logic.auth as auth
import ckan.plugins as p


def csvtocsvw_annotate(context, data_dict):
    if "resource" in data_dict and data_dict["resource"].get("package_id"):
        data_dict["id"] = data_dict["resource"].get("package_id")
        privilege = "package_update"
    else:
        privilege = "resource_update"
    return auth.datastore_auth(context, data_dict, privilege=privilege)


def csvtocsvw_transform(context, data_dict):
    if "resource" in data_dict and data_dict["resource"].get("package_id"):
        data_dict["id"] = data_dict["resource"].get("package_id")
        privilege = "package_update"
    else:
        privilege = "resource_update"
    return auth.datastore_auth(context, data_dict, privilege=privilege)


def get_auth_functions():
    return {
        "csvtocsvw_annotate": csvtocsvw_annotate,
        "csvtocsvw_transform": csvtocsvw_transform,
    }
