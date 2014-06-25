import requests

endpoints = {
    "upload": "/upload/{file_path}",
    "get": "/get/{file_path}"
    }

class Client(object):
    """Client for elliptics fastcgi proxy."""
    def __init__(self, proxy):
        self.proxy_url = "http://{0}:{1}".format(proxy.host, proxy.port)

    def get_url(self, service, params=None, **kwargs):
        url = "{proxy}/{uri}".format(proxy=self.proxy_url, uri=endpoints[service])
        url = url.format(**kwargs)
        if params is not None:
            url += '&'.join("{}:{}".format(k, v) for k, v in params.items())
        return url

    def upload(self, path, data, params=None):
        url = self.get_url("upload", params=params, file_path=path)
        result = requests.post(url, data=data)
        result.raise_for_status()

    def get(self, path, params=None):
        url = self.get_url("get", params=params, file_path=path)
        result = requests.get(url)
        result.raise_for_status()
        return result.content
