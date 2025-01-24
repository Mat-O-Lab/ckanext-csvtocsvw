import ckan.plugins.toolkit as toolkit

def csvtocsvw_show_tools(resource):
    default_formats = toolkit.config.get("ckanext.csvtocsvw.formats").lower().split()
    if resource['format'].lower() in default_formats:
        return True
    else:
        False


def get_helpers():
    return {
        "csvtocsvw_show_tools": csvtocsvw_show_tools,
    }
