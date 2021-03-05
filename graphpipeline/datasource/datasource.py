import json
import os
import logging
import shutil
from datetime import datetime
from uuid import uuid4

import requests
import importlib
from collections import namedtuple

from dateutil.parser import parse

from graphpipeline.datasource.helper.downloader import list_ftp_dir

log = logging.getLogger(__name__)


def download_latest(datasource_class_name: str, import_path: str, root_dir: str, download_arguments: dict = None):
    # set empty download_arguments dictionary if not passed
    if not download_arguments:
        download_arguments = {}
    module = importlib.import_module(import_path)
    datasource_class = getattr(module, datasource_class_name)

    ds = datasource_class(root_dir)
    ds.download(ds.latest_remote_version(), **download_arguments)
    
    return ds.ds_dir


def download_latest_if_not_exists(datasource_class_name: str, import_path: str, root_dir: str, download_arguments: dict = None):
    # set empty download_arguments dictionary if not passed
    if not download_arguments:
        download_arguments = {}
    module = importlib.import_module(import_path)
    datasource_class = getattr(module, datasource_class_name)
    ds = datasource_class(root_dir)

    if not ds.latest_local_instance(**download_arguments):
        ds.download(ds.latest_remote_version(), **download_arguments)
    else:
        log.info(f'Found local instance at {ds.latest_local_instance(**download_arguments).instance_dir}')

    return ds.ds_dir


class BaseDataSource():

    def __init__(self, root_dir):
        self.root_dir = root_dir

        self.name = self.__class__.__name__

        # set the data source directory (using class name)
        self._ds_dir = os.path.join(self.root_dir, self.__class__.__name__)
        if not os.path.exists(self.ds_dir):
            os.mkdir(self.ds_dir)

    def to_dict(self):
        return dict(
            root_dir=self.root_dir,
            name=self.name,
            ds_dir=self.ds_dir
        )

    @classmethod
    def from_dict(cls, d):
        ds = cls(d['root_dir'])
        ds.ds_dir = d['ds_dir']
        ds.name = d['name']
        return ds

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

    def latest_local_instance(self, **download_arguments):
        """
        Get local instance with latest 'instance_created' property.

        :return: The latest local instance.
        """
        if not download_arguments:
            download_arguments = {}
        latest = None
        for instance in self.instances_local:
            if instance.download_arguments == download_arguments:
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
        print(args, kwargs)
        self.pre_download()

        instance = DataSourceInstance(self)
        instance.started = datetime.now()
        instance.download_date = datetime.today()
        instance.download_arguments = kwargs

        try:
            instance.prepare_download()

            # run the download function defined in the implementing class.
            self.download_function(instance, **kwargs)

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
        instance.download_arguments = kwargs

        instance.version = str(version)

        try:
            print(instance)
            print('download')
            print(kwargs)
            instance.prepare_download()

            self.download_function(instance, version, **kwargs)

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
        instance.download_arguments = kwargs

        instance.version = str(version)

        try:
            instance.prepare_download()

            if self.version_downloadable(version):
                # run the download function defined in the implementing class.
                self.download_function(instance, version, **kwargs)
            instance.wrap_up()

        except:
            instance.on_error()
            raise
        finally:
            self.post_download()

        return instance


class DataSourceInstance():

    def __init__(self, datasource, download_arguments = None, datasource_directory=None, uuid=None):
        self.datasource = datasource

        if not uuid:
            self.uuid = str(uuid4())
        else:
            self.uuid = uuid

        if datasource_directory:
            self.ds_dir = datasource_directory
        else:
            self.ds_dir = self.datasource.ds_dir

        if download_arguments:
            self.download_arguments = download_arguments
        else:
            self.download_arguments = {}

        # property for time of instantiation
        self.instance_created = datetime.now()

        self.instance_dir = os.path.join(self.ds_dir, self.uuid)
        self.process_instance_dir = os.path.join(self.ds_dir, "process_{}".format(self.uuid))

    def to_dict(self) -> dict:
        """
        Serialize into a dictionary to e.g. pass into multiprocessing.

        :return: Serialized dictionary
        """
        return dict(
            datasource=self.datasource.to_dict(),
            download_arguments=self.download_arguments,
            uuid=self.uuid,
            instance_created=self.instance_created,
            process_instance_dir=self.process_instance_dir
        )

    @classmethod
    def from_dict(cls, d: dict) -> 'DataSourceInstance':
        datasourceinstance = cls(BaseDataSource.from_dict(d['datasource']), download_arguments=d['download_arguments'], uuid=d['uuid'])
        datasourceinstance.instance_created = d['instance_created']

        return datasourceinstance

    @classmethod
    def read(cls, datasource, path):
        if os.path.exists(path):
            # read metadata
            with open(os.path.join(path, 'metadata.json'), 'r') as f:
                properties = json.load(f)

            # format properties (read dates, other types)
            formatted_properties = {}
            for k, v in properties.items():
                # try to read dates, this should ideally be an Encode/Decode for json.load()
                # only if it looks like a date string ... (not a good filter)
                if v:
                    if all(x in v for x in ['-', ':', '.']):
                        try:
                            parsed_date = parse(v)
                            formatted_properties[k] = parsed_date
                        except (ValueError, TypeError):
                            formatted_properties[k] = v
                    else:
                        formatted_properties[k] = v

            # create instance and set attributes
            datasourceinstance = cls(datasource, uuid=formatted_properties['uuid'])

            for k, v in formatted_properties.items():
                # TODO make sure not to store the directories in metadata instead of skipping here
                if '_dir' not in k:
                    setattr(datasourceinstance, k, v)

            return datasourceinstance

    def prepare_download(self):
        """
        Create temporary directory where the downloaded files are stored.

        Directory is usually renamed when all downloads finished without error.
        """
        if not os.path.exists(self.process_instance_dir):
            os.mkdir(self.process_instance_dir)

    def store(self):
        instance_metadata_path = os.path.join(self.process_instance_dir, 'metadata.json')

        output_dict = {}

        for k, v in self.__dict__.items():
            if not k.startswith('__') and not k.startswith('datasource') and not callable(v) and '_dir' not in k:
                output_dict[k] = getattr(self, k)

        with open(instance_metadata_path, 'w') as f:
            json.dump(output_dict, f, indent=4, sort_keys=True, default=str)

    def wrap_up(self):
        """
        Wrap up Instance when all downloads finished.
        """
        self.finished = datetime.now()
        self.store()
        os.rename(self.process_instance_dir, self.instance_dir)

    def on_error(self):
        """
        Rename Instance directory to indicate error.
        """
        error_instance_path = os.path.join(self.ds_dir, "error_{}".format(self.uuid))
        os.rename(self.process_instance_dir, error_instance_path)

    def get_file(self, filename):
        """
        Try to get a particular file from downloaded data.

        :param filename: Name of file.
        :return: Path to file.
        """
        log.info("Get a single file {}".format(filename))

        files_found = self.get_files(filename)

        if len(files_found) == 1:
            log.debug(f"Single file found: {files_found[0]}")
            return files_found[0]
        elif len(files_found) > 1:
            raise ValueError("Found multiple files with the same name!")
        else:
            log.debug("File not found, return None.")

    def get_directory(self, directory_name):
        """
        Try to get a particular directory from downloaded data.

        :param directory_name: Name of directory.
        :return: Path to directory.
        """
        log.debug("Get a single direcotry {}".format(directory_name))

        directories_found = self.get_directories(directory_name)

        if len(directories_found) == 1:
            return directories_found[0]
        elif len(directories_found) > 1:
            raise ValueError("Found multiple directories with the same name!")

    def get_files(self, filename):
        """
        Try to get files from downloaded data.

        :param filename: Name of file.
        :return: List of paths to file.
        """
        log.info("Get files: {0}".format(filename))
        log.info("From DataSource directory: {0}".format(self.instance_dir))

        files_found = []

        for (dirname, subdirs, files) in os.walk(self.instance_dir):
            for f in files:
                if f == filename:
                    files_found.append(
                        os.path.join(dirname, f)
                    )

        return files_found

    def get_file_from_directory(self, directory_name, filename):
        """
        Get a specific filename from a specific directory.

        This can only be one file, duplicate files are not possible.

        E.g.:
            /path/to/data/mouse/data.zip
            /path/to/data/human/data.zip

        :param directory_name: The name of the directory containing the file.
        :param filename: The name of the file.
        :return:
        """
        log.info("Get file {0} in directory {1}".format(filename, directory_name))
        log.info("From DataSource directory: {0}".format(self.instance_dir))

        files_found = []

        for (dirname, subdirs, files) in os.walk(self.instance_dir):
            if os.path.basename(dirname) == directory_name:
                for f in files:
                    if f == filename:
                        files_found.append(
                            os.path.join(dirname, f)
                        )

        if len(files_found) == 1:
            return files_found[0]
        elif len(files_found) > 1:
            raise ValueError("Found multiple files with same name in directory!")

    def get_directories(self, directory_name):
        """
        Try to get directories from downloaded data.

        :param directory_name: Name of directory.
        :return: List of paths to directories.
        """
        log.debug("Get directory: {0}".format(directory_name))
        log.debug("From DataSource directory: {0}".format(self.instance_dir))

        directories_found = []

        dir_path = self.instance_dir
        for (dirname, subdirs, files) in os.walk(dir_path):
            for d in subdirs:
                if d == directory_name:
                    directories_found.append(
                        os.path.join(dirname, d)
                    )

        return directories_found

    # TODO fix and adapt
    def find_files(self, filter_func, version, subpath=None):
        """
        Find files where the filter criterion is True.

        Set an optional subpath relative to the directory of this version.

        :param filter_func: Filter function (input is filename, return boolean)
        :param subpath: Optional subpath where to search for file.
        :return: List of files that match the filter.
        """

        log.debug("Find files in DataSource")
        log.debug("From DataSource directory: {0}".format(self.instance_dir))

        if subpath:
            this_path = os.path.join(self.instance_dir, subpath)
        else:
            this_path = self.instance_dir

        files_found = []

        for (dirname, subdirs, files) in os.walk(this_path):
            for f in files:
                if filter_func(f) is True:
                    files_found.append(
                        os.path.join(dirname, f)
                    )

        return files_found