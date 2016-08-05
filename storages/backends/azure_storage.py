from datetime import datetime
import os.path
import mimetypes
import time
from time import mktime

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


def clean_name(name):
    return os.path.normpath(name).replace("\\", "/")


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
            self._connection = BlobService(
                self.account_name, self.account_key)
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
            get_blob = self.connection.get_blob_to_bytes
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
            self.azure_container, name)
        return properties["content-length"]

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
            return "{}{}/{}".format(cdn_url, self.azure_container, name)
        if hasattr(self.connection, 'make_blob_url'):
            return self.connection.make_blob_url(
                container_name=self.azure_container,
                blob_name=name,
                protocol=self.azure_protocol,
            )
        else:
            return "{}{}/{}".format(setting('MEDIA_URL'), self.azure_container, name)

    def modified_time(self, name):
        try:
            modified = self.__get_blob_properties(name)['last-modified']
        except (TypeError, KeyError):
            return super(AzureStorage, self).modified_time(name)

        modified = time.strptime(modified, '%a, %d %b %Y %H:%M:%S %Z')
        modified = datetime.fromtimestamp(mktime(modified))

        return modified
