import ckan.plugins.toolkit as tk


@tk.auth_allow_anonymous_access
def csvtocsvw_get_sum(context, data_dict):
    return {"success": True}


def get_auth_functions():
    return {
        "csvtocsvw_get_sum": csvtocsvw_get_sum,
    }
