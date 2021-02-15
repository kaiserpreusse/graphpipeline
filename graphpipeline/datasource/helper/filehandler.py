import logging
import os
import subprocess
import zipfile
import shutil
import gzip
import tarfile
import dateutil.parser
from io import BufferedWriter

log = logging.getLogger(__name__)


def _dirname_from_datetime(some_datetime):
    """
    Create a dirname without spaces/colon/dot from a datetime.datetime

    :param some_datetime: The datetime.datetime.now()
    :return: A directory name.
    """
    return str(some_datetime).replace(' ', '___').replace(':', '__').replace('.', '_')


def _datetime_from_dirname(dirname):
    """
    Get the datetime.datetime from a directory name.

    :param dirname:
    :return:
    """
    return dateutil.parser.parse(dirname.replace('___', ' ').replace('__', ':').replace('_', '.'))


def existing(path):
    """
    Make sure a path exists. Takes a path and returns it, creating it if not existing.

    :param path: The path
    :return: The path (which now exists)
    """
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def unpack_gzip(gz_file_path, target_directory=None, remove=None):
    """
    Unpack .gz file to target directory (or same directory if none provided).

    :param gz_file_path:
    :param target_directory:
    :param remove: True/False - Remove source file after unpacking
    :return: Path of unpacked file.
    """
    gz_file_name = os.path.basename(gz_file_path)
    gz_file_base_directory = os.path.dirname(gz_file_path)

    unpacked_file_name = gz_file_name.replace('.gz', '')
    unpacked_file_path = os.path.join(target_directory, unpacked_file_name)

    if not target_directory:
        target_directory = gz_file_base_directory

    with gzip.open(gz_file_path, 'rb') as f_in:
        with open(unpacked_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    return unpacked_file_path


def unpack_tar(tar_file_path, target_directory=None, remove=None):
    """

    :param tar_file_path:
    :param target_directory:
    :param remove:
    :return:
    """
    tar_file_name = os.path.basename(tar_file_path)
    tar_file_base_directory = os.path.dirname(tar_file_path)

    if not target_directory:
        target_directory = tar_file_base_directory

    tar = tarfile.open(tar_file_path)
    tar.extractall(path=target_directory)
    tar.close()


def unzip(zip_file_path, target_directory=None):
    """
    Unzip a zip file to the target directory. If no target directory provided, the directory of the zip file is used.

    The function creates a new subdirectory with the name of the zip file.

    By default, an existing target directory is *not* overwritten.

    :param filepath: Path to zip file
    :param target_directory: Target directory to store the unzipped files.
    :return: Path to the directory with extracted content.
    """
    # process zip file name: check if Buffer or other data type and extract the file path
    if isinstance(zip_file_path, BufferedWriter):
        zip_file_path = zip_file_path.name

    log.debug('Unzip {}'.format(zip_file_path))
    zip_file_name = os.path.basename(zip_file_path)
    zip_file_base_directory = os.path.dirname(zip_file_path)

    # set name of target directory if not passed
    if not target_directory:
        target_directory = zip_file_base_directory

    # extract to /target_directory/zip_file_name/
    extract_directory = os.path.join(target_directory, zip_file_name.replace('.zip', ''))

    log.debug('To directory {}'.format(extract_directory))

    with zipfile.ZipFile(zip_file_path, 'r') as source_zip:
        source_zip.extractall(extract_directory)

    return extract_directory
