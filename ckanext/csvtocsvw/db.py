"""
Job database for csvtocsvw: stores the log records of annotate/transform jobs
so they can be shown on the resource pages.

Loosely based on ckan-service-provider's db.py (same tables as
ckanext-csvwmapandtransform and ckanext-fuseki).
"""

import datetime
import json

import six
import sqlalchemy
from ckan.plugins import toolkit

ENGINE = None
_METADATA = None
JOBS_TABLE = None
METADATA_TABLE = None
LOGS_TABLE = None


def init(db_uri: str = "", echo=False):
    """Initialise the database.

    Initialise the sqlalchemy engine, metadata and table objects that we use to
    connect to the database.

    Create the database and the database tables themselves if they don't
    already exist.

    :param uri: the sqlalchemy database URI
    :type uri: string

    :param echo: whether or not to have the sqlalchemy engine log all
        statements to stdout
    :type echo: bool

    """
    if not db_uri:
        db_uri = toolkit.config.get("ckanext.csvtocsvw.db_url")
    global ENGINE, _METADATA, JOBS_TABLE, METADATA_TABLE, LOGS_TABLE
    ENGINE = sqlalchemy.create_engine(db_uri, echo=echo)
    _METADATA = sqlalchemy.MetaData()
    JOBS_TABLE = _init_jobs_table()
    METADATA_TABLE = _init_metadata_table()
    LOGS_TABLE = _init_logs_table()
    _METADATA.create_all(ENGINE)


def drop_all():
    """Delete all the database tables (if they exist).

    This is for tests to reset the DB. Note that this will delete *all* tables
    in the database, not just tables created by this module (for example
    apscheduler's tables will also be deleted).

    """
    if _METADATA:
        _METADATA.drop_all(ENGINE)


def delete_job(job_id):
    """Delete a job from the jobs table by job_id.

    :param job_id: the job_id of the job to be deleted
    :type job_id: unicode
    """
    if job_id:
        job_id = six.text_type(job_id)

    msg = ""
    with ENGINE.connect() as conn:
        trans = conn.begin()
        try:
            result = conn.execute(
                JOBS_TABLE.delete().where(JOBS_TABLE.c.job_id == job_id)
            )
            if result.rowcount == 0:
                msg = f"No job found with id: {job_id}"
            else:
                msg = f"Job with id: {job_id} has been deleted successfully."
            trans.commit()
        except Exception as e:
            trans.rollback()
            msg = f"An error occurred: {e}"
    return msg


def get_job(job_id):
    """Return the job with the given job_id as a dict."""
    if job_id:
        job_id = six.text_type(job_id)

    with ENGINE.connect() as conn:
        result = conn.execute(
            JOBS_TABLE.select().where(JOBS_TABLE.c.job_id == job_id)
        ).first()

    if not result:
        return None

    result_dict = {
        field: (
            value.isoformat()
            if isinstance(value := result._mapping[field], datetime.datetime)
            else value
        )
        for field in result._mapping.keys()
    }

    result_dict["metadata"] = _get_metadata(job_id)
    result_dict["logs"] = _get_logs(job_id)

    return result_dict


def add_pending_job(job_id, job_type, data=None, metadata=None, result_url=None):
    """Add a new job with status "pending" to the jobs table."""
    if not data:
        data = {}
    data = six.text_type(json.dumps(data))

    if job_id:
        job_id = six.text_type(job_id)
    if job_type:
        job_type = six.text_type(job_type)
    if result_url:
        result_url = six.text_type(result_url)

    if not metadata:
        metadata = {}

    with ENGINE.connect() as conn:
        trans = conn.begin()
        try:
            conn.execute(
                JOBS_TABLE.insert().values(
                    job_id=job_id,
                    job_type=job_type,
                    status="pending",
                    requested_timestamp=datetime.datetime.utcnow(),
                    sent_data=data,
                    result_url=result_url,
                )
            )

            inserts = [
                {
                    "job_id": job_id,
                    "key": six.text_type(key),
                    "value": six.text_type(
                        json.dumps(value)
                        if not isinstance(value, six.string_types)
                        else value
                    ),
                    "type": (
                        "json" if not isinstance(value, six.string_types) else "string"
                    ),
                }
                for key, value in metadata.items()
            ]

            if inserts:
                conn.execute(METADATA_TABLE.insert(), inserts)
            trans.commit()
        except Exception:
            trans.rollback()
            raise


class InvalidErrorObjectError(Exception):
    pass


def _validate_error(error):
    """Validate and return the given error object.

    Based on the given error object, return either None or a dict with a
    "message" key whose value is a string (the dict may also have any other
    keys that it wants).

    The given "error" object can be:

    - None, in which case None is returned

    - A string, in which case a dict like this will be returned:
      {"message": error_string}

    - A dict with a "message" key whose value is a string, in which case the
      dict will be returned unchanged

    :param error: the error object to validate

    :raises InvalidErrorObjectError: If the error object doesn't match any of
        the allowed types

    """
    if error is None:
        return None
    elif isinstance(error, six.string_types):
        return {"message": error}
    else:
        try:
            message = error["message"]
            if isinstance(message, six.string_types):
                return error
            else:
                raise InvalidErrorObjectError("error['message'] must be a string")
        except (TypeError, KeyError):
            raise InvalidErrorObjectError(
                "error must be either a string or a dict with a message key"
            )


def _update_job(job_id, job_dict):
    """Update the database row for the given job_id with the given job_dict."""
    if job_id:
        job_id = six.text_type(job_id)

    if "error" in job_dict:
        job_dict["error"] = json.dumps(_validate_error(job_dict["error"]))
        job_dict["error"] = six.text_type(job_dict["error"])

    if "data" in job_dict:
        job_dict["data"] = six.text_type(job_dict["data"])

    with ENGINE.begin() as conn:
        conn.execute(
            JOBS_TABLE.update().where(JOBS_TABLE.c.job_id == job_id).values(**job_dict)
        )


def mark_job_as_completed(job_id, data=None):
    """Mark a job as completed successfully.

    :param job_id: the job_id of the job to be updated
    :type job_id: unicode

    :param data: the output data returned by the job
    :type data: any JSON-serializable type (including None)

    """
    update_dict = {
        "status": "complete",
        "data": json.dumps(data),
        "finished_timestamp": datetime.datetime.utcnow(),
    }
    _update_job(job_id, update_dict)


def mark_job_as_missed(job_id):
    """Mark a job as missed because it was in the queue for too long.

    :param job_id: the job_id of the job to be updated
    :type job_id: unicode

    """
    update_dict = {
        "status": "error",
        "error": "Job delayed too long, service full",
        "finished_timestamp": datetime.datetime.utcnow(),
    }
    _update_job(job_id, update_dict)


def mark_job_as_errored(job_id, error_object):
    """Mark a job as failed with an error.

    :param job_id: the job_id of the job to be updated
    :type job_id: unicode

    :param error_object: the error returned by the job
    :type error_object: either a string or a dict with a "message" key whose
        value is a string

    """
    update_dict = {
        "status": "error",
        "error": error_object,
        "finished_timestamp": datetime.datetime.utcnow(),
    }
    _update_job(job_id, update_dict)


def mark_job_as_failed_to_post_result(job_id):
    """Mark a job as 'failed to post result'.

    This happens when a job completes (either successfully or with an error)
    then trying to post the job result back to the job's callback URL fails.

    FIXME: This overwrites any error from the job itself!

    :param job_id: the job_id of the job to be updated
    :type job_id: unicode

    """
    update_dict = {
        "error": "Process completed but unable to post to result_url",
    }
    _update_job(job_id, update_dict)


def _init_jobs_table():
    """Initialise the "jobs" table in the db."""
    _jobs_table = sqlalchemy.Table(
        "jobs",
        _METADATA,
        sqlalchemy.Column("job_id", sqlalchemy.UnicodeText, primary_key=True),
        sqlalchemy.Column("job_type", sqlalchemy.UnicodeText),
        sqlalchemy.Column("status", sqlalchemy.UnicodeText, index=True),
        sqlalchemy.Column("data", sqlalchemy.UnicodeText),
        sqlalchemy.Column("error", sqlalchemy.UnicodeText),
        sqlalchemy.Column("requested_timestamp", sqlalchemy.DateTime),
        sqlalchemy.Column("finished_timestamp", sqlalchemy.DateTime),
        sqlalchemy.Column("sent_data", sqlalchemy.UnicodeText),
        # Callback URL:
        sqlalchemy.Column("result_url", sqlalchemy.UnicodeText),
    )
    return _jobs_table


def _init_metadata_table():
    """Initialise the "metadata" table in the db."""
    _metadata_table = sqlalchemy.Table(
        "metadata",
        _METADATA,
        sqlalchemy.Column(
            "job_id",
            sqlalchemy.ForeignKey("jobs.job_id", ondelete="CASCADE"),
            nullable=False,
            primary_key=True,
        ),
        sqlalchemy.Column("key", sqlalchemy.UnicodeText, primary_key=True),
        sqlalchemy.Column("value", sqlalchemy.UnicodeText, index=True),
        sqlalchemy.Column("type", sqlalchemy.UnicodeText),
    )
    return _metadata_table


def _init_logs_table():
    """Initialise the "logs" table in the db."""
    _logs_table = sqlalchemy.Table(
        "logs",
        _METADATA,
        sqlalchemy.Column(
            "job_id",
            sqlalchemy.ForeignKey("jobs.job_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sqlalchemy.Column("timestamp", sqlalchemy.DateTime),
        sqlalchemy.Column("message", sqlalchemy.UnicodeText),
        sqlalchemy.Column("level", sqlalchemy.UnicodeText),
        sqlalchemy.Column("module", sqlalchemy.UnicodeText),
        sqlalchemy.Column("funcName", sqlalchemy.UnicodeText),
        sqlalchemy.Column("lineno", sqlalchemy.Integer),
    )
    return _logs_table


def _get_metadata(job_id):
    """Return any metadata for the given job_id from the metadata table."""
    job_id = six.text_type(job_id)

    with ENGINE.connect() as conn:
        results = conn.execute(
            METADATA_TABLE.select().where(METADATA_TABLE.c.job_id == job_id)
        ).fetchall()

    metadata = {
        row["key"]: json.loads(row["value"]) if row["type"] == "json" else row["value"]
        for row in (r._mapping for r in results)
    }
    return metadata


def _get_logs(job_id):
    """Return any logs for the given job_id from the logs table."""
    job_id = six.text_type(job_id)

    with ENGINE.connect() as conn:
        results = conn.execute(
            LOGS_TABLE.select().where(LOGS_TABLE.c.job_id == job_id)
        ).fetchall()

    results = [dict(result._mapping) for result in results]

    for result in results:
        result.pop("job_id")

    return results
