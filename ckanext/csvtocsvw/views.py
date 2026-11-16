from flask import Blueprint

blueprint = Blueprint("csvtocsvw", __name__)


import json

import ckan.lib.base as base
import ckan.lib.helpers as core_helpers
import ckan.plugins.toolkit as toolkit
from ckan.common import _
from flask import Blueprint
from flask.views import MethodView
from ckanext.csvtocsvw.tasks import resource_search
from ckanext.csvtocsvw.helpers import service_available

log = __import__("logging").getLogger(__name__)


def _decorate_logs(status):
    """Add display attributes (icon, css class, relative time) to the job log."""
    for item in (status or {}).get("logs", []):
        item["timestamp"] = core_helpers.time_ago_from_timestamp(item["timestamp"])
        if item["level"] == "DEBUG":
            item["alertlevel"], item["icon"], item["class"] = "info", "bug-slash", "success"
        elif item["level"] == "INFO":
            item["alertlevel"], item["icon"], item["class"] = "info", "check", "success"
        else:
            item["alertlevel"], item["icon"], item["class"] = "error", "exclamation", "failure"
    return status


class AnnotateStatusView(MethodView):
    """JSON status and log of the annotate job (polled by the page)."""

    def get(self, id: str, resource_id: str):
        try:
            status = toolkit.get_action("csvtocsvw_status")(
                {}, {"resource_id": resource_id, "task_key": "csvtocsvw_annotate"}
            )
        except toolkit.ObjectNotFound:
            base.abort(404, "Resource not found")
        except toolkit.NotAuthorized:
            base.abort(403, _("Not authorized to see this page"))
        return {"status": _decorate_logs(status)}


class TransformStatusView(MethodView):
    """JSON status and log of the transform job (polled by the page)."""

    def get(self, id: str, resource_id: str):
        try:
            resource = toolkit.get_action("resource_show")({}, {"id": resource_id})
            meta_res = resource_search(
                {},
                id,
                name=resource["name"].rsplit(".")[0] + "-metadata.jsonld",
            )
            status = {}
            if meta_res:
                status = toolkit.get_action("csvtocsvw_status")(
                    {},
                    {"resource_id": meta_res["id"], "task_key": "csvtocsvw_transform"},
                )
        except toolkit.ObjectNotFound:
            base.abort(404, "Resource not found")
        except toolkit.NotAuthorized:
            base.abort(403, _("Not authorized to see this page"))
        return {"status": _decorate_logs(status)}


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
                "status_url": toolkit.url_for(
                    "csvtocsvw.csv_annotate_status", id=id, resource_id=resource_id
                ),
                "service_status": service_available(),
            },
        )


class TransformView(MethodView):
    def post(self, id: str, resource_id: str):
        try:
            resource = toolkit.get_action("resource_show")({}, {"id": resource_id})
            meta_res = resource_search(
                {},
                id,
                name=resource["name"].rsplit(".")[0] + "-metadata.jsonld",
                hadPrimarySource=resource["url"],
            )
            if meta_res:
                toolkit.get_action("csvtocsvw_transform")(
                    {}, {"resource_id": meta_res["id"]}
                )
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
            meta_res = resource_search(
                {},
                id,
                name=resource["name"].rsplit(".")[0] + "-metadata.jsonld",
                # hadPrimarySource=resource["url"],
            )
        except (toolkit.ObjectNotFound, toolkit.NotAuthorized):
            base.abort(404, "Resource not found")
        status = None
        try:
            task = toolkit.get_action("task_status_show")(
                {},
                {
                    "entity_id": meta_res["id"],
                    "task_type": "csvtocsvw",
                    "key": "csvtocsvw_transform",
                },
            )
        except:
            status = None
        else:
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
                "meta_res": meta_res,
                "resource": resource,
                "status": status,
                "status_url": toolkit.url_for(
                    "csvtocsvw.csv_transform_status", id=id, resource_id=resource_id
                ),
                "service_status": service_available(),
            },
        )


blueprint.add_url_rule(
    "/dataset/<id>/resource/<resource_id>/csv_annotate/status",
    view_func=AnnotateStatusView.as_view(str("csv_annotate_status")),
)

blueprint.add_url_rule(
    "/dataset/<id>/resource/<resource_id>/csv_transform/status",
    view_func=TransformStatusView.as_view(str("csv_transform_status")),
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
