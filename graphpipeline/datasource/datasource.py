import os
import logging
import shutil
from datetime import datetime
from uuid import uuid4
from urllib.parse import urlparse, urljoin
import requests
from requests.auth import HTTPBasicAuth
import importlib
import json
from enum import Enum
import posixpath
from ftputil import FTPHost

from dateutil.parser import parse

from graphpipeline.datasource.helper.downloader import list_ftp_dir, _read_text_file_ftp, get_links, download_directory_from_http, download_directory_from_ftp

log = logging.getLogger(__name__)


class RemoteProtocols(Enum):
    FTP = 'ftp'
    WEBDAV = 'webdav'
    HTTP = 'http'
    HTTPS = 'https'


def _http_list_remote_instances(url, root_dir, user=None, password=None):
    log.debug(f"List remote instances for {url}")
    log.debug(f"Auth: {user}, {password}")
    remote_instances = []

    if user and password:
        r = requests.get(url, auth=HTTPBasicAuth(user, password))
    else:
        r = requests.get(url)
    if r.status_code != 200:
        raise Exception('status code is {} for {}'.format(r.status_code, url))
    content = r.text

    for link in get_links(content):
        if not link.startswith('.') and not link.startswith('error_'):  # skip hidden files such as .DS_Store
            log.debug(f"Link found: {link}")
            instance_url = urljoin(url, link)
            json_url = urljoin(instance_url, 'metadata.json')
            log.debug(f"JSON URL {json_url}")

            if user and password:
                instance_json = requests.get(json_url, auth=HTTPBasicAuth(user, password))
            else:
                instance_json = requests.get(json_url)

            if instance_json.status_code != 200:
                raise Exception('status code is {} for {}'.format(r.status_code, url))
            content = instance_json.text

            remote_instances.append(
                DataSourceInstance.from_dict(json.loads(content), root_dir)
            )

    return remote_instances


def _ftp_list_remote_instances(url, root_dir, user=None, password=None):
    log.debug(f"Get remote instances from {url}")
    parsed_url = urlparse(url)
    list_of_items = list_ftp_dir(parsed_url.netloc, parsed_url.path, user=user, password=password)

    remote_instances = []

    for item in list_of_items:

        if item.permissions.startswith('d') and not item.name.startswith('error_'):
            json_path = posixpath.join(parsed_url.path, item.name, 'metadata.json')
            json_url = urljoin(f'ftp://{parsed_url.netloc}', json_path)
            log.debug(f'Retrieve instance from {json_url}')
            json_file_like = _read_text_file_ftp(json_url, user=user, password=password)

            json_dict = json.loads(json_file_like.getvalue())
            remote_instances.append(
                DataSourceInstance.from_dict(json_dict, root_dir)
            )

    return remote_instances


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


def setify_dict(d: dict) -> dict:
    """
    In order to compare download arguments read from JSON we have to load lists into sets.

    So that:
        {taxids: ['10090', '9606']} == {taxids: ['9606', '10090'[}

    :param d: The dictionary
    :return: Dictionary with list as set.
    """
    new_d = {}
    for k,v in d.items():
        if isinstance(v, list):
            new_d[k] = set(v)
        else:
            new_d[k] = v
    return new_d


class BaseDataSource():

    def __init__(self, root_dir, name=None):
        self.root_dir = root_dir

        if name:
            self.name = name
        else:
            self.name = self.__class__.__name__

        # set the data source directory (using class name)
        self._ds_dir = os.path.join(self.root_dir, self.name)

        try:
            if not os.path.exists(self.ds_dir):
                os.mkdir(self.ds_dir)
        except FileNotFoundError:
            log.error(f"Datasource class tried to create {self.ds_dir} but failed.")

    def to_dict(self):
        return dict(
            name=self.name
        )

    @classmethod
    def from_dict(cls, d, root_dir):
        ds = cls(root_dir, name=d['name'])

        return ds

    @classmethod
    def from_dir(cls, root_dir):
        """
        Create DataSource instances from a root_dir. Skip common files.
        """
        output = []
        for name in os.listdir(root_dir):
            if not name.startswith('.'):
                output.append(
                    BaseDataSource(root_dir, name)
                )
        return output

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

    def list_remote_instances(self, url, protocol, user: str = None, password: str = None):
        log.debug(f"List remote instances on {protocol} ressource at {url}.")
        if protocol == RemoteProtocols.HTTP.value:
            if not url.endswith('/'):
                url = url+'/'
            datasource_url = url+self.name+'/'

            return _http_list_remote_instances(datasource_url, self.root_dir, user=user, password=password)

        elif protocol == RemoteProtocols.FTP.value:
            if not url.endswith('/'):
                url = url+'/'
            datasource_url = url+self.name+'/'

            return _ftp_list_remote_instances(datasource_url, self.root_dir, user=user, password=password)

    def latest_remote_instance(self, url, protocol, user: str = None, password: str = None, argument_check: bool = True, **download_arguments):
        """
        Get local instance with latest 'instance_created' property.

        :return: The latest local instance.
        """
        if not download_arguments:
            download_arguments = {}
        latest = None

        # only return instances where the download arguments match (default, argument_check = True)
        if argument_check:
            for instance in self.list_remote_instances(url, protocol, user, password):

                if setify_dict(instance.download_arguments) == setify_dict(download_arguments):
                    if not latest:
                        latest = instance
                    else:
                        if instance.instance_created > latest.instance_created:
                            latest = instance
        # return all latest instances, don't check for download arguments (argument_check = False)
        else:
            for instance in self.list_remote_instances(url, protocol, user, password):
                if not latest:
                    latest = instance
                else:
                    if instance.instance_created > latest.instance_created:
                        latest = instance

        return latest

    def pull_latest_from_remote(self, url, protocol, user=None, password=None, argument_check: bool = True, **download_arguments):
        log.debug(f"Pull latest remote version of {self.name} from {url}")
        latest_remote_instance = self.latest_remote_instance(url, protocol, user=user, password=password, argument_check=argument_check, **download_arguments)

        if latest_remote_instance:
            datasource_url = posixpath.join(url, self.name)
            log.debug(f"Datasource URL {datasource_url}")
            latest_remote_instance_url = posixpath.join(datasource_url, latest_remote_instance.uuid)
            log.debug(f"Instance URL: {latest_remote_instance_url}")

            if protocol == RemoteProtocols.FTP.value:
                parsed_url = urlparse(latest_remote_instance_url)
                download_directory_from_ftp(parsed_url.netloc, parsed_url.path, os.path.join(self.ds_dir, latest_remote_instance.uuid), user=user, password=password)
            elif protocol == RemoteProtocols.HTTP.value:
                download_directory_from_http(latest_remote_instance_url, os.path.join(self.ds_dir, latest_remote_instance.uuid))

    def push_to_remote(self, url, protocol, user=None, password=None, overwrite=False):
        """
        Push all instances of this datasource to a remote server.

        Currently only supports FTP.

        :param url: The target URL
        :param protocol: Protocol (currently only FTP)
        :param user: Username
        :param password: Password
        :param overwrite: If True, overwrite if target exists. Default is False.
        """
        log.debug(f"Push all instances of {self.__class__.__name__} to {url}.")

        datasource_url = posixpath.join(url, self.name)

        parsed_url = urlparse(datasource_url)
        if protocol == RemoteProtocols.FTP.value:
            with FTPHost(parsed_url.netloc, user, password) as ftp:
                pass




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

    def latest_local_instance(self, argument_check: bool = True, **download_arguments):
        """
        Get local instance with latest 'instance_created' property.

        :return: The latest local instance.
        """
        if not download_arguments:
            download_arguments = {}
        latest = None

        # only return instances where the download arguments match (default, argument_check = True)
        if argument_check:
            for instance in self.instances_local:
                if setify_dict(instance.download_arguments) == setify_dict(download_arguments):
                    if not latest:
                        latest = instance
                    else:
                        if instance.instance_created > latest.instance_created:
                            latest = instance
        # return all latest instances, don't check for download arguments (argument_check = False)
        else:
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

    def latest_remote_version(self):
        return None

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
        if not kwargs:
            kwargs = {}

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

        self.version = None

        # property for time of instantiation
        self.instance_created = datetime.now()

        self.instance_dir = os.path.join(self.ds_dir, self.uuid)
        self.process_instance_dir = os.path.join(self.ds_dir, "process_{}".format(self.uuid))

    def to_dict(self) -> dict:
        """
        Serialize into a dictionary to e.g. pass into multiprocessing.

        :return: Serialized dictionary
        """
        d = dict(
            datasource=self.datasource.to_dict(),
            download_arguments=self.download_arguments,
            uuid=self.uuid,
            instance_created=self.instance_created,
            version=self.version
        )

        return d

    @classmethod
    def from_dict(cls, d: dict, root_dir) -> 'DataSourceInstance':
        """
        Create a DataSourceInstance from dict. The standard metadata.json file
        contains file paths for the underlying datasource. This can be overwritten
        for remote contexts.

        :param d:
        :param root_dir:
        :return:
        """
        datasource = BaseDataSource(root_dir, name=d['datasource']['name'])

        datasourceinstance = cls(datasource, download_arguments=d['download_arguments'], uuid=d['uuid'])
        datasourceinstance.instance_created = d['instance_created']
        datasourceinstance.version = d['version']

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
                # don't overwrite directories or the datasource
                if '_dir' not in k and 'datasource' not in k:
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

        with open(instance_metadata_path, 'w') as f:
            json.dump(self.to_dict(), f, indent=4, sort_keys=True, default=str)

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

    def find_files(self, filter_func, subpath=None):
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
