import json
import os
from uuid import uuid4
from datetime import datetime
import logging

from dateutil.parser import parse

log = logging.getLogger(__name__)


class DataSourceInstance():

    def __init__(self, datasource, uuid=None):
        self.datasource = datasource

        if not uuid:
            self.uuid = str(uuid4())
        else:
            self.uuid = uuid

        self.ds_dir = self.datasource.ds_dir

        # property for time of instantiation
        self.instance_created = datetime.now()

        self.instance_dir = os.path.join(self.ds_dir, self.uuid)
        self.process_instance_dir = os.path.join(self.ds_dir, "process_{}".format(self.uuid))

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
            datasourceinstance = cls(datasource, formatted_properties['uuid'])

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
            if not k.startswith('__') and not callable(v) and isinstance(v, str) and '_dir' not in k:
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
