import datetime
import os.path
import mimetypes
import time
from time import mktime

import requests
from django.core.files.base import ContentFile
from django.core.exceptions import ImproperlyConfigured
from storages.compat import Storage

try:
    import azure  # noqa
except ImportError:
    raise ImproperlyConfigured(
        "Could not load Azure bindings. "
        "See https://github.com/WindowsAzure/azure-sdk-for-python")

try:
    # azure-storage 0.31.0
    from azure.storage.blob.blockblobservice import BlockBlobService as BlobService
    from azure.common import AzureMissingResourceHttpError
    from azure.storage.blob.models import ContentSettings
except ImportError as err:
    try:
        # azure-storage 0.20.0
        from azure.storage.blob.blobservice import BlobService
        from azure.common import AzureMissingResourceHttpError
    except ImportError:
        from azure.storage import BlobService
        from azure import WindowsAzureMissingResourceError as AzureMissingResourceHttpError

from storages.utils import setting
from storages.backends.helpers.ectoken3 import encrypt_v3


def clean_name(name):
    return os.path.normpath(name).replace("\\", "/")


class AzureAdapter(requests.adapters.HTTPAdapter):
    def send(self, *args, **kwargs):
        if 'timeout' not in kwargs:
            kwargs['timeout'] = 5
        return super(AzureAdapter, self).send(*args, **kwargs)


class AzureStorage(Storage):
    account_name = setting("AZURE_ACCOUNT_NAME")
    account_key = setting("AZURE_ACCOUNT_KEY")
    azure_container = setting("AZURE_CONTAINER")
    azure_ssl = setting("AZURE_SSL")

    def __init__(self, *args, **kwargs):
        super(AzureStorage, self).__init__(*args, **kwargs)
        self._connection = None

    @property
    def connection(self):
        if self._connection is None:
            session = requests.Session()
            adapter = AzureAdapter(max_retries=10)
            session.mount('https://', adapter)
            self._connection = BlobService(
                self.account_name, self.account_key, request_session=session)
        return self._connection

    @property
    def azure_protocol(self):
        if self.azure_ssl:
            return 'https'
        return 'http' if self.azure_ssl is not None else None

    def __get_blob_properties(self, name):
        try:
            return self.connection.get_blob_properties(
                self.azure_container,
                name
            )
        except AzureMissingResourceHttpError:
            return None

    def _open(self, name, mode="rb"):
        if hasattr(self.connection, 'get_blob'):
            get_blob = self.connection.get_blob
        else:
            def get_blob(azure_container, blob_name):
                return self.connection.get_blob_to_bytes(azure_container, blob_name).content
        contents = get_blob(self.azure_container, name)
        return ContentFile(contents)

    def exists(self, name):
        return self.__get_blob_properties(name) is not None

    def delete(self, name):
        try:
            self.connection.delete_blob(self.azure_container, name)
        except AzureMissingResourceHttpError:
            pass

    def size(self, name):
        properties = self.connection.get_blob_properties(
            self.azure_container, name).properties
        return properties.content_length

    def _azure_3_save(self, name, content_type, content_data):
        content_type = ContentSettings(content_type=content_type)
        self.connection.create_blob_from_bytes(self.azure_container, name,
                                               content_data,
                                               content_settings=content_type)

    def _azure_2_save(self, name, content_type, content_data):
        self.connection.put_blob(self.azure_container, name,
                                 content_data, "BlockBlob",
                                 x_ms_blob_content_type=content_type)

    def _save(self, name, content):
        if hasattr(content.file, 'content_type'):
            content_type = content.file.content_type
        else:
            content_type = mimetypes.guess_type(name)[0]

        if hasattr(content, 'chunks'):
            content_data = b''.join(chunk for chunk in content.chunks())
        else:
            content_data = content.read()

        if hasattr(self.connection, 'put_blob'):
            self._azure_2_save(name, content_type, content_data)
        else:
            self._azure_3_save(name, content_type, content_data)

        return name

    def url(self, name):
        cdn_url = setting('AZURE_CDN_URL')
        if cdn_url:
           full_url = "{}{}/{}".format(cdn_url, self.azure_container, name)
        elif hasattr(self.connection, 'make_blob_url'):
            return self.connection.make_blob_url(
                container_name=self.azure_container,
                blob_name=name,
                protocol=self.azure_protocol,
            )
        else:
            full_url = "{}{}/{}".format(setting('MEDIA_URL'), self.azure_container, name)
        key = setting('AZURE_CDN_TOKEN_KEY')
        timeout = setting('AZURE_CDN_TOKEN_TIMEOUT')
        if key and timeout:
            # Get GMT timestamp.
            timestamp = int(datetime.datetime(*(time.gmtime()[:-3])).strftime("%s"))
            full_url += '?{}'.format(encrypt_v3(key, 'ec_expire={}'.format(timestamp + timeout)))
        return full_url

    def modified_time(self, name):
        try:
            modified = self.__get_blob_properties(name)['last-modified']
        except (TypeError, KeyError):
            return super(AzureStorage, self).modified_time(name)

        modified = time.strptime(modified, '%a, %d %b %Y %H:%M:%S %Z')
        modified = datetime.datetime.fromtimestamp(mktime(modified))

        return modified

    def listdir(self, path):
        blobs = self.connection.list_blobs(self.azure_container)
        return [], [blob.name for blob in blobs]
