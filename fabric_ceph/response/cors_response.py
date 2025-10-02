import json
import os
from typing import Union

from flask import request, Response

from fabric_ceph.openapi_server.models import Version, Status500InternalServerErrorErrors, Status500InternalServerError, \
    Status404NotFoundErrors, Status404NotFound, Status403ForbiddenErrors, Status403Forbidden, \
    Status401UnauthorizedErrors, Status401Unauthorized, Status400BadRequestErrors, Status400BadRequest, \
    Status200OkNoContentData, Status200OkNoContent

_INDENT = int(os.getenv('OC_API_JSON_RESPONSE_INDENT', '4'))


def delete_none(_dict):
    """
    Delete None values recursively from all of the dictionaries, tuples, lists, sets
    """
    if isinstance(_dict, dict):
        for key, value in list(_dict.items()):
            if isinstance(value, (list, dict, tuple, set)):
                _dict[key] = delete_none(value)
            elif value is None or key is None:
                del _dict[key]

    elif isinstance(_dict, (list, set, tuple)):
        _dict = type(_dict)(delete_none(item) for item in _dict if item is not None)

    return _dict


def cors_response(req: request, status_code: int = 200, body: object = None, x_error: str = None) -> Response:
    """
    Return CORS Response object
    """
    response = Response()
    response.status_code = status_code
    response.data = body
    response.headers['Access-Control-Allow-Origin'] = req.headers.get('Origin', '*')
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, PATCH, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = \
        'DNT, User-Agent, X-Requested-With, If-Modified-Since, Cache-Control, Content-Type, Range, Authorization'
    response.headers['Access-Control-Expose-Headers'] = 'Content-Length, Content-Range, X-Error'

    if x_error:
        response.headers['X-Error'] = x_error

    return response


def cors_200(response_body: Union[Version] = None) -> Response:
    """
    Return 200 - OK
    """
    body = json.dumps(delete_none(response_body.to_dict()), indent=_INDENT, sort_keys=True) \
        if _INDENT != 0 else json.dumps(delete_none(response_body.to_dict()), sort_keys=True)
    return cors_response(
        req=request,
        status_code=200,
        body=body
    )


def cors_200_no_content(details: str = None) -> Response:
    """
    Return 200 - No Content
    """
    data = Status200OkNoContentData()
    data.details = details
    data_object = Status200OkNoContent([data])
    return cors_response(
        req=request,
        status_code=200,
        body=json.dumps(delete_none(data_object.to_dict()), indent=_INDENT, sort_keys=True)
        if _INDENT != 0 else json.dumps(delete_none(data_object.to_dict()), sort_keys=True),
        x_error=details
    )


def cors_400(details: str = None) -> Response:
    """
    Return 400 - Bad Request
    """
    errors = Status400BadRequestErrors()
    errors.details = details
    error_object = Status400BadRequest([errors])
    return cors_response(
        req=request,
        status_code=400,
        body=json.dumps(delete_none(error_object.to_dict()), indent=_INDENT, sort_keys=True)
        if _INDENT != 0 else json.dumps(delete_none(error_object.to_dict()), sort_keys=True),
        x_error=details
    )


def cors_401(details: str = None) -> Response:
    """
    Return 401 - Unauthorized
    """
    errors = Status401UnauthorizedErrors()
    errors.details = details
    error_object = Status401Unauthorized([errors])
    return cors_response(
        req=request,
        status_code=401,
        body=json.dumps(delete_none(error_object.to_dict()), indent=_INDENT, sort_keys=True)
        if _INDENT != 0 else json.dumps(delete_none(error_object.to_dict()), sort_keys=True),
        x_error=details
    )


def cors_403(details: str = None) -> Response:
    """
    Return 403 - Forbidden
    """
    errors = Status403ForbiddenErrors()
    errors.details = details
    error_object = Status403Forbidden([errors])
    return cors_response(
        req=request,
        status_code=403,
        body=json.dumps(delete_none(error_object.to_dict()), indent=_INDENT, sort_keys=True)
        if _INDENT != 0 else json.dumps(delete_none(error_object.to_dict()), sort_keys=True),
        x_error=details
    )


def cors_404(details: str = None) -> Response:
    """
    Return 404 - Not Found
    """
    errors = Status404NotFoundErrors()
    errors.details = details
    error_object = Status404NotFound([errors])
    return cors_response(
        req=request,
        status_code=404,
        body=json.dumps(delete_none(error_object.to_dict()), indent=_INDENT, sort_keys=True)
        if _INDENT != 0 else json.dumps(delete_none(error_object.to_dict()), sort_keys=True),
        x_error=details
    )


def cors_500(details: str = None) -> Response:
    """
    Return 500 - Internal Server Error
    """
    errors = Status500InternalServerErrorErrors()
    errors.details = details
    error_object = Status500InternalServerError([errors])
    return cors_response(
        req=request,
        status_code=500,
        body=json.dumps(delete_none(error_object.to_dict()), indent=_INDENT, sort_keys=True)
        if _INDENT != 0 else json.dumps(delete_none(error_object.to_dict()), sort_keys=True),
        x_error=details
    )
