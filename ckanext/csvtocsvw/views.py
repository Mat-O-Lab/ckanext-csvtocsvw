from flask import Blueprint

blueprint = Blueprint("csvtocsvw", __name__)


from flask import Blueprint
from flask.views import MethodView
import ckan.plugins.toolkit as toolkit
import ckan.lib.helpers as core_helpers
import ckan.lib.base as base
import json
from ckan.common import _

log = __import__("logging").getLogger(__name__)


class AnnotateView(MethodView):
    def post(self, id: str, resource_id: str):
        try:
            toolkit.get_action("csvtocsvw_annotate")({}, {"resource_id": resource_id})
        except toolkit.ObjectNotFound:
            base.abort(404, "Resource not found")
        except toolkit.NotAuthorized:
            base.abort(403, _("Not authorized to see this page"))
        except toolkit.ValidationError:
            log.debug(toolkit.ValidationError)

        return core_helpers.redirect_to(
            "csvtocsvw.csv_annotate", id=id, resource_id=resource_id
        )

    def get(self, id: str, resource_id: str):
        try:
            pkg_dict = toolkit.get_action("package_show")({}, {"id": id})
            resource = toolkit.get_action("resource_show")({}, {"id": resource_id})

            # backward compatibility with old templates
            toolkit.g.pkg_dict = pkg_dict
            toolkit.g.resource = resource
            status = None
            task = toolkit.get_action("task_status_show")(
                {},
                {
                    "entity_id": resource["id"],
                    "task_type": "csvtocsvw",
                    "key": "csvtocsvw_annotate",
                },
            )
        except toolkit.ObjectNotFound:
            base.abort(404, "Resource not found")
        except toolkit.NotAuthorized:
            base.abort(403, _("Not authorized to see this page"))
        
        value = json.loads(task["value"])
        job_id = value.get("job_id")
        url = None
        try:
            error = json.loads(task["error"])
        except ValueError:
            # this happens occasionally, such as when the job times out
            error = task["error"]
        status = {
            "status": task["state"],
            "job_id": job_id,
            "job_url": url,
            "last_updated": task["last_updated"],
            "error": error,
        }

        return base.render(
            "csvtocsvw/csv_annotate.html",
            extra_vars={
                "pkg_dict": pkg_dict,
                "resource": resource,
                "status": status,
            },
        )


class TransformView(MethodView):
    def post(self, id: str, resource_id: str):
        try:
            toolkit.get_action("csvtocsvw_transform")({}, {"resource_id": resource_id})
        except toolkit.ObjectNotFound:
            base.abort(404, "Resource not found")
        except toolkit.NotAuthorized:
            base.abort(403, _("Not authorized to see this page"))
        except toolkit.ValidationError:
            log.debug(toolkit.ValidationError)
        return core_helpers.redirect_to(
            "csvtocsvw.csv_transform", id=id, resource_id=resource_id
        )

    def get(self, id: str, resource_id: str):
        try:
            pkg_dict = toolkit.get_action("package_show")({}, {"id": id})
            resource = toolkit.get_action("resource_show")({}, {"id": resource_id})

            # backward compatibility with old templates
            toolkit.g.pkg_dict = pkg_dict
            toolkit.g.resource = resource

            status = None
            task = toolkit.get_action("task_status_show")(
                {},
                {
                    "entity_id": resource["id"],
                    "task_type": "csvtocsvw",
                    "key": "csvtocsvw_transform",
                },
            )
        except toolkit.ObjectNotFound:
            base.abort(404, "Resource not found")
        except toolkit.NotAuthorized:
            base.abort(403, _("Not authorized to see this page"))
        except toolkit.ValidationError:
            log.debug(toolkit.ValidationError)
        
        value = json.loads(task["value"])
        job_id = value.get("job_id")
        url = None
        try:
            error = json.loads(task["error"])
        except ValueError:
            # this happens occasionally, such as when the job times out
            error = task["error"]
        status = {
            "status": task["state"],
            "job_id": job_id,
            "job_url": url,
            "last_updated": task["last_updated"],
            "error": error,
            "task": task,
        }

        return base.render(
            "csvtocsvw/csv_transform.html",
            extra_vars={
                "pkg_dict": pkg_dict,
                "resource": resource,
                "status": status,
            },
        )


blueprint.add_url_rule(
    "/dataset/<id>/resource/<resource_id>/csv_annotate",
    view_func=AnnotateView.as_view(str("csv_annotate")),
)

blueprint.add_url_rule(
    "/dataset/<id>/resource/<resource_id>/csv_transform",
    view_func=TransformView.as_view(str("csv_transform")),
)

def get_blueprint():
    return blueprint
