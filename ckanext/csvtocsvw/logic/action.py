import ckan.plugins.toolkit as tk
import ckanext.csvtocsvw.logic.schema as schema


@tk.side_effect_free
def csvtocsvw_get_sum(context, data_dict):
    tk.check_access(
        "csvtocsvw_get_sum", context, data_dict)
    data, errors = tk.navl_validate(
        data_dict, schema.csvtocsvw_get_sum(), context)

    if errors:
        raise tk.ValidationError(errors)

    return {
        "left": data["left"],
        "right": data["right"],
        "sum": data["left"] + data["right"]
    }


def get_actions():
    return {
        'csvtocsvw_get_sum': csvtocsvw_get_sum,
    }
