import os
import logging
import shutil
from datetime import datetime
import requests

from graphpipeline.datasource.datasourceinstance import DataSourceInstance
from graphpipeline.datasource.helper.downloader import list_ftp_dir

log = logging.getLogger(__name__)


class BaseDataSource():

    def __init__(self, root_dir):
        self.root_dir = root_dir

        self.name = self.__class__.__name__

        # set the data source directory (using class name)
        self._ds_dir = os.path.join(self.root_dir, self.__class__.__name__)
        if not os.path.exists(self.ds_dir):
            os.mkdir(self.ds_dir)

    @property
    def ds_dir(self):
        return self._ds_dir

    @ds_dir.setter
    def ds_dir(self, value):
        self._ds_dir = value

    def list_instances(self):
        return os.listdir(self.ds_dir)

    @property
    def instances_local(self):
        """
        Load all instances of this DataSource.
        """
        for instance in self.list_instances():
            if 'error_' not in instance and 'process_' not in instance and not instance.startswith('.'):
                yield self.get_instance_by_uuid(instance)

    def clear_datasource_directory(self):
        """
        Delete all local instances.
        """
        log.debug("Clear DataSource directory {}".format(self.ds_dir))
        for instance in self.list_instances():
            if not instance.startswith('.'):
                instance_path = os.path.join(self.ds_dir, instance)
                shutil.rmtree(instance_path)

    def get_instance_by_uuid(self, uuid):
        instance_path = os.path.join(self.ds_dir, uuid)

        if os.path.exists(instance_path):
            return DataSourceInstance.read(self, instance_path)

    def latest_local_instance(self):
        """
        Get local instance with latest 'instance_created' property.

        :return: The latest local instance.
        """
        latest = None
        for instance in self.instances_local:
            if not latest:
                latest = instance
            else:
                if instance.instance_created > latest.instance_created:
                    latest = instance
        return latest


class FileSystemDataSource(BaseDataSource):

    def __init__(self, root_dir):
        super(FileSystemDataSource, self).__init__(root_dir)


class RemoteDataSource(BaseDataSource):

    def __init__(self, root_dir):
        super(RemoteDataSource, self).__init__(root_dir)
        self.remote_files = []

    def download(self):
        raise NotImplementedError

    def download_function(self):
        raise NotImplementedError

    def pre_download(self):
        pass

    def post_download(self):
        pass

    def current_downloads_in_process(self):
        """
        Return a list of current instances that are in process.
        """
        instances_in_process = []
        for instance in self.list_instances():
            if instance.startswith('process_'):
                instances_in_process.append(instance)
        return instances_in_process

    def check_remote_files(self):
        """
        Check if all URLs registered at self.remote_files are available.

        :return: dict with available/unavailable URLs.
        """
        out = {'found': [], 'not_found': []}
        for url in self.remote_files:

            if url.startswith('http://') or url.startswith('https://'):
                try:
                    status_code = requests.head(url, allow_redirects=True).status_code
                    if status_code == 200:
                        out['found'].append(url)
                    else:
                        out['not_found'].append(url)
                except Exception as e:
                    out['not_found'].append(url)

            elif url.startswith('ftp://'):
                try:
                    # get the parent directory
                    # remove trailing slash in directory names
                    if url.endswith('/'):
                        formatted_url = url[:-1]
                    else:
                        formatted_url = url
                    # get parent directory
                    parent_url, filename = formatted_url.strip().rsplit('/', 1)
                    print(parent_url, filename)
                    files = list_ftp_dir(parent_url)
                    if filename in [f.name for f in files]:
                        out['found'].append(url)
                except Exception as e:
                    out['not_found'].append(url)

            else:
                out['not_found'].append(url)

        return out

class RollingReleaseRemoteDataSource(RemoteDataSource):

    def __init__(self, root_dir):
        super(RollingReleaseRemoteDataSource, self).__init__(root_dir)

    def download(self, *args, **kwargs):
        self.pre_download()

        instance = DataSourceInstance(self)
        instance.started = datetime.now()
        instance.download_date = datetime.today()

        try:
            instance.prepare_download()

            # run the download function defined in the implementing class.
            self.download_function(instance, *args, **kwargs)

            instance.wrap_up()
        except:
            instance.on_error()
            raise
        finally:
            self.post_download()

        return instance


class ManyVersionsRemoteDataSource(RemoteDataSource):

    def __init__(self, root_dir):
        super(ManyVersionsRemoteDataSource, self).__init__(root_dir)

    def all_remote_versions(self):
        raise NotImplementedError

    def latest_remote_version(self):
        return max(self.all_remote_versions())

    def version_downloadable(self, version):
        """
        Check if version is downloadable.
        """
        if version in self.all_remote_versions():
            return True

    def download(self, version, *args, **kwargs):
        """
        Download a specific version.

        :param version: The version.
        :param taxids: Optional list of taxonomy IDs to limit download.
        :type version: DataSourceVersion
        """
        self.pre_download()

        instance = DataSourceInstance(self)
        instance.started = datetime.now()
        instance.download_date = datetime.today()

        instance.version = str(version)

        try:
            instance.prepare_download()

            self.download_function(instance, version, *args, **kwargs)

            instance.wrap_up()
        except:
            instance.on_error()
            raise
        finally:
            self.post_download()

        return instance


class SingleVersionRemoteDataSource(RemoteDataSource):
    """
    DataSource class for a remote data source with defined versions where only
    one (usually the last one) version can be downloaded.

    The DataSource knows the specific name of the accessible version and returns on
    all version functions.
    """

    def __init__(self, root_dir):
        super(SingleVersionRemoteDataSource, self).__init__(root_dir)

    def latest_remote_version(self):
        raise NotImplementedError

    def version_downloadable(self, version):
        """
        Check if version is downloadable.
        """
        if version == self.latest_remote_version():
            return True

    def download(self, *args, **kwargs):
        self.pre_download()
        version = self.latest_remote_version()

        instance = DataSourceInstance(self)
        instance.started = datetime.now()
        instance.download_date = datetime.today()

        instance.version = str(version)

        try:
            instance.prepare_download()

            if self.version_downloadable(version):
                # run the download function defined in the implementing class.
                self.download_function(instance, version, *args, **kwargs)
            instance.wrap_up()

        except:
            instance.on_error()
            raise
        finally:
            self.post_download()

        return instance
