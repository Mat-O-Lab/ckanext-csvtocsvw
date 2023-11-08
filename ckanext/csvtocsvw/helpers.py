

def csvtocsvw_show_tools(resource):
    from ckanext.csvtocsvw.plugin import DEFAULT_FORMATS
    if resource['format'].lower() in DEFAULT_FORMATS:
        return True
    else:
        False


def get_helpers():
    return {
        "csvtocsvw_show_tools": csvtocsvw_show_tools,
    }
