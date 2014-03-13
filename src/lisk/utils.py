import requests
import base64
import json
import threading

import settings


def _request(url, method='get', **kwargs):
    print 'making request to:', url, kwargs
    auth = requests.auth.HTTPBasicAuth(
        settings.API_USERNAME, settings.API_PASSWORD)
    return getattr(requests, method)(url, auth=auth, **kwargs)


def _json_request(url, **kwargs):
    response = _request(url, **kwargs)
    return json.loads(response.content)


def _restful_iterator(transformer, url, start_offset,
                      limit=settings.API_LIMIT):
    more = True
    offset = start_offset
    while more:
        query = {'offset': offset, 'limit': limit}
        response = _json_request(url, params=query)
        if not response['meta']['next']:
            more = False
        for obj in response['objects']:
            transformed = transformer(obj)
            if transformed:
                yield transformed
        offset += settings.API_LIMIT


def get_spaces(transformer, offset=0):
    """
    Returns a generator to iterate over spaces.
    Each json object in the response will be sent to the transformer
    which can transform the json object to another python object
    """
    url = '%s/spaces/' % settings.BASE_API_URL
    for obj in _restful_iterator(transformer, url, offset):
        yield obj


def get_projects(transformer, offset=0):
    url = '%s/projects/' % settings.BASE_API_URL
    for obj in _restful_iterator(transformer, url, offset):
        yield obj


def get_space_projects(transformer, space_id, offset=0):
    #TODO: actually use spaces/id/projects once it works
    def _transformer(obj):
        if obj['space']['id'] == space_id:
            return transformer(obj)
        return None
    url = '%s/projects/' % settings.BASE_API_URL
    for obj in _restful_iterator(_transformer, url, offset):
        yield obj


def get_project_files(transformer, project_id, offset=0):
    url = '%s/projects/%s/files/' % (settings.BASE_API_URL, project_id)
    for obj in _restful_iterator(transformer, url, offset):
        yield obj


def get_file_content(file_id):
    url = '%s/files/%s/content/' % (settings.BASE_API_URL, file_id)
    return _request(url).content


def put_file_content(file_id, content):
    url = '%s/files/%s/content/' % (settings.BASE_API_URL, file_id)
    return _request(url, 'put', data=content)


def post_file(file_json):
    url = '%s/files/' % (settings.BASE_API_URL)
    import pdb;pdb.set_trace()
    return _request(url, 'post', data=json.dumps(file_json),
                    headers={'content-type': 'application/json'})
