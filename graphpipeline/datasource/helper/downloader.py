import datetime
import logging
import os
import io
from collections import namedtuple
from ftplib import FTP
from urllib.parse import urlparse, urljoin
from pathlib import Path
from requests.auth import HTTPBasicAuth

from bs4 import BeautifulSoup
from ftputil import FTPHost
import ftputil
import requests
from dateutil import parser
from ftputil.error import FTPIOError

log = logging.getLogger(__name__)


##############################################################
# download functions
##############################################################

def download_file_to_dir(remote_url, local_path, filename=None, overwrite=None):
    """
    Download a file (with full URL) to a local directory.

    :param remote_url: URL to file on server.
    :param local_path: Local path.
    :param filename: Filename (optional, last part of URL if not defined)
    :param overwrite: Set if existing file is overwritten.
    :return: Path to downloaded file.
    :rtype: str
    """
    log.debug("Download single file. Overwrite is {0}".format(overwrite))

    # use filename provided or get from URL
    if filename:
        downloaded_file_path = os.path.join(local_path, filename)
    else:
        downloaded_file_path = os.path.join(local_path, remote_url.split('/')[-1])

    # download if overwrite is True
    if overwrite:
        if os.path.isfile(downloaded_file_path):
            os.remove(downloaded_file_path)

        return download_file(remote_url, downloaded_file_path)

    # else download only if file does not exist
    else:
        if not os.path.isfile(downloaded_file_path):
            return download_file(remote_url, downloaded_file_path)
        else:
            log.debug("Return path to existing file: {0}".format(downloaded_file_path))
            return downloaded_file_path


def download_file(remote_url, local_file_path):
    """
    Download a single file. Needs full remote URL and full local path including filename.

    :param remote_url: Full path to file on server.
    :param local_file_path: Full local target for file.
    :return: Path to local file.
    :rtype: str
    """
    log.debug("Download {0} to {1}".format(remote_url, local_file_path))

    local_path = os.path.dirname(local_file_path)
    if not os.path.exists(local_path):
        os.makedirs(local_path)

    if remote_url.startswith('http'):
        # catch 302 temporary redirect
        # necessary if http redirects to ftp
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/302
        r = requests.head(remote_url, allow_redirects=False)
        if r.status_code == 302:
            location = r.headers['Location']
            log.debug("302 redirect to {} found.".format(location))
            if location.startswith('ftp://'):
                return _download_file_ftp(location, local_file_path)

        return _download_file_http(remote_url, local_file_path)

    elif remote_url.startswith('ftp://'):
        return _download_file_ftp(remote_url, local_file_path)

    else:
        raise AttributeError("Neither downloadable http(s) or ftp URL: {0}".format(remote_url))


def _download_file_http(u, path):
    r = requests.get(u, stream=True)

    if r.status_code == 200:
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()
        return path

    else:
        raise ValueError("URL can't be retrieved. Status code: {0}. URL: {1}".format(r.status_code, u))


def _read_text_file_ftp(url, user=None, password=None) -> io.StringIO:
    """
    Read content of a single file from an FTP server. This returns a io.StringIO
    instance. Use only for uncompressed text files,
    does not return meaningful output for gzipped files or other binary files.
    """
    retries = 3

    # add 'ftp://' to form a parsable URL in case a path is passed
    if not url.startswith("ftp://"):
        url = 'ftp://' + url

    log.debug('Read FTP file {}'.format(url))

    ftp_url = urlparse(url)

    log.debug("Parsed URL: {0}".format(ftp_url))

    # try download n times, return if successful
    for i in range(retries):
        try:
            ftp = FTP(ftp_url.netloc)

            if user:
                ftp.login(user=user, passwd=password)
            else:
                ftp.login()
            log.debug(f'execute RETR on {ftp_url.path}')

            output = io.StringIO()

            ftp.retrlines("RETR {0}".format(ftp_url.path), output.write)
            ftp.close()
            return output

        except EOFError as e:
            log.error(f"Download of {ftp_url.path} not successful on try {i+1}, try again.")
            log.error(e)

    raise ValueError(f"File {ftp_url.path} not available")


def _download_file_ftp(url, filepath, user=None, pw=None):
    """
    Read content of a single file from an FTP server.

    Return text if possible.
    """
    retries = 3

    # add 'ftp://' to form a parsable URL in case a path is passed
    if not url.startswith("ftp://"):
        url = 'ftp://' + url

    log.debug('Download FTP file {}'.format(url))
    log.debug('Target {}'.format(filepath))

    ftp_url = urlparse(url)

    log.debug("Parsed URL: {0}".format(ftp_url))

    # try download n times, return if successful
    for i in range(retries):
        try:
            ftp = FTP(ftp_url.netloc)

            if user:
                ftp.login(user=user, passwd=pw)
            else:
                ftp.login()

            with open(filepath, 'wb') as f:

                ftp.retrbinary("RETR {0}".format(ftp_url.path), f.write)

            ftp.close()
            return filepath

        except EOFError as e:
            log.error(f"Download of {ftp_url.path} not successful on try {i+1}, try again.")
            log.error(e)

    raise ValueError(f"File {ftp_url.path} not available")


def download_directory_from_http(url, target, user=None, password=None):
    """
    Download all files from a http directory to a local directory recursively.

    Note that the first call *has to end with /* otherwise a file with the name of the root dir will be
    created instead of the directories.
    """
    log.debug(f"Download all files from {url} to {target}")

    r = requests.get(url)
    if r.status_code != 200:
        raise Exception('status code is {} for {}'.format(r.status_code, url))
    content = r.text
    if url.endswith('/'):
        Path(target).mkdir(parents=True, exist_ok=True)
        for link in get_links(content):
            if not link.startswith('.'): # skip hidden files such as .DS_Store
                download_directory_from_http(urljoin(url, link), os.path.join(target, link), user, password)
    else:
        with open(target, 'w') as f:
            f.write(content)


def download_directory_from_ftp(remote_url, source, target, user=None, password=None, overwrite=None, file_blacklist=None,
                                dir_blacklist=None, dir_whitelist=None):
    """
    Recursively download a directory from FTP server.

    The function first excludeds everything from blacklist and then filters whitelist if it is passed.

    :param pw:
    :param user:
    :param source:
    :param remote_url:
    :param target:
    :param overwrite:
    :return:
    """
    if not dir_blacklist:
        dir_blacklist = []

    if not file_blacklist:
        file_blacklist = []

    if not dir_whitelist:
        dir_whitelist = []

    log.debug("Download directory '{0}' from '{1}' to '{2}'".format(source, remote_url, target))
    log.debug("Overwrite is {0}".format(overwrite))
    log.debug("File Blacklist: {}".format(file_blacklist))
    log.debug("Dir Blacklist: {}".format(dir_blacklist))
    log.debug("Dir Whitelist: {}".format(dir_whitelist))

    if not remote_url.startswith('ftp://'):
        remote_url = f'ftp://{remote_url}'

    ftp_url = urlparse(remote_url)

    log.debug("FTP URL parsed: {}".format(ftp_url))

    if not os.path.exists(target):
        os.makedirs(target)

    if not user:
        user = 'anonymous'

    local_files = []

    with ftputil.FTPHost(ftp_url.netloc, user, password) as ftp_host:
        log.debug("FTP connnected: {0}".format(str(ftp_host)))
        source = source.rstrip('/')
        target = target.rstrip('/')
        for (dirname, subdirs, files) in ftp_host.walk(source):
            log.debug(f'{dirname}, {subdirs}, {files}')

            # get name of this dir without source
            # + 1 removes the leading slash
            this_target = dirname[len(source) + 1:]

            # only download if no element of dir_blacklist is in path
            # the any() function is hard to read
            # it iterates dir_blacklist, checks if the element is in the string 'dirname'
            # if this is never True [= not any()] we go on
            if not any(x in dirname for x in dir_blacklist):
                # continue if whitelist and dirname not in whitelist
                if dir_whitelist and any(x in dirname for x in dir_whitelist):
                    continue
                log.debug('Directory: {}, {}'.format(dirname, this_target))
                this_target_dir = os.path.join(target, this_target)

                if not os.path.exists(this_target_dir):
                    os.mkdir(this_target_dir)

                for f in files:
                    if f not in file_blacklist:
                        remote_file = os.path.join(dirname, f)
                        local_file = os.path.join(this_target_dir, f)

                        if overwrite:
                            ftp_host.download(remote_file, local_file)
                        else:
                            if not os.path.isfile(local_file):
                                try:
                                    ftp_host.download(remote_file, local_file)
                                except FTPIOError:
                                    log.debug('Cannot download: {0}'.format(remote_file))
                            else:
                                log.debug("File exists and 'overwrite' is {0}: {1}".format(overwrite, local_file))
                        local_files.append(local_file)
                    else:
                        log.debug("Skip {}".format(this_target))
            else:
                log.debug("Skip {}".format(this_target))

    return local_files


def get_single_file_ftp(url, user=None, pw=None):
    """
    Read content of a single file from an FTP server.

    Return text if possible.
    """
    # add 'ftp://' to form a parsable URL in case a path is passed
    if not url.startswith("ftp://"):
        url = 'ftp://' + url

    log.debug("Get content of single file from FTP.")
    log.debug("User: {0}, Password: {1}".format(user, pw))
    log.debug("Input URL: {0}".format(url))

    ftp_url = urlparse(url)

    log.debug("Parsed URL: {0}".format(ftp_url))

    ftp = FTP(ftp_url.netloc)

    if user:
        ftp.login(user=user, passwd=pw)
    else:
        ftp.login()

    output = io.BytesIO()

    ftp.retrbinary("RETR {0}".format(ftp_url.path), output.write)

    ftp.close()

    output.seek(0)

    return output


##############################################################
# FTP helper functions
##############################################################


def list_ftp_dir(url, path=None, user=None, password=None):
    """
    List a FTP directory.

    !!! Note the issue with last modified date for directories and links, see _date_from_ftp_file() !!!

    :param base_url: FTP URL for login
    :param path: Optional path on FTP Server for CWD.
    :return: Directory list.
    """
    filelist = raw_list_ftp_dir(url, path=path, user=user, password=password)
    return [_parse_ftp_list_item(x) for x in filelist]


def list_files_only_ftp_dir(url, path=None):
    """
    List only files in FTP directory (i.e. skip links and directories).

    :param url: FTP URL for login
    :param path: Optional path on FTP Server for CWD.
    :return: List of files
    """
    output = []

    itemlist = raw_list_ftp_dir(url, path=path)

    for i in itemlist:
        flds = i.split()

        # if not directory
        if not flds[0].startswith('d'):
            # if not link
            if len(flds) >= 10 and flds[9] == '->':
                pass
            else:
                output.append(_parse_ftp_list_item(i))

    return output


def raw_list_ftp_dir(url, path=None, user=None, password=None):
    """
    Get the raw output of FTP LIST on an FTP path.

    :param url: FTP URL for login
    :param path: Optional path on FTP Server for CWD.
    :return: Raw directory list.
    """
    log.debug("List '{0}', optional path '{1}'".format(url, path))

    if not url.startswith("ftp://"):
        url = 'ftp://' + url

    ftp_url = urlparse(url)
    ftp = FTP(ftp_url.netloc)

    if user and password:
        ftp.login(user=user, passwd=password)
    else:
        ftp.login()

    # move into path of url
    if ftp_url.path:
        ftp.cwd(ftp_url.path)

    # additional CWD if a path is given separately
    if path:
        ftp.cwd(path)
    filelist = []
    ftp.retrlines('LIST', callback=filelist.append)

    return filelist


def _date_from_ftp_file(ftp_file_line):
    """
    Get the modification date from a line of 'LIST' FTP.

    The year field contains the year for files modified earlier than the current year.

    For files modified the current year, it contains the time (hh:mm).

    Note for links:
    There is an issue with links to other files: In FTP LIST command they seem to always show the
    last modified time (hh:mm) and not the year (even if the link was modified earlier than current year).

    FTP listing in browsers shows the correct year. You have to get the last modified date from the target of
    the link to show the correct date.

    Note for directories:
    Similar to links, the last modified time is always returned from FTP list.

    !!! For now we do not handle this, any outer function using this must make sure to NOT PASS links and directories
    if this is an issue. !!!

    :param ftp_file_line: The line in 'LIST' from an FTP server.
    :return: The date modified
    :rtype: datetime.date
    """

    months = {
        "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05",
        "Jun": "06", "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10",
        "Nov": "11", "Dec": "12"
    }

    today = datetime.date.today()

    flds = ftp_file_line.split()

    month = flds[5]
    month_num = months[month]
    day = flds[6]
    year = flds[7]

    # if file is a link, do not return a date
    if ':' in year:
        year = today.year
    return datetime.date(int(year), int(month_num), int(day))


def _parse_ftp_list_item(ftp_file_line):
    """
    Get the modification date from a line of 'LIST' FTP.

    This currently works for standard UNIX output only.

        'drwxr-xr-x   4 ftp      ftp          4096 Dec 19  2007 10.1'


    :param ftp_file_line: The line in 'LIST' from an FTP server.
    :return: The date modified
    :rtype: datetime.date
    """

    flds = ftp_file_line.split()
    FtpFile = namedtuple('FtpFile', ['permissions', 'owner', 'group', 'size', 'date', 'name'])

    return FtpFile(flds[0], flds[2], flds[3], flds[4], _date_from_ftp_file(ftp_file_line), flds[8])


def latest_date_version_ftp(url, path=None):
    """
    Get the latest date changed on URL/path.

    :param url: URL.
    :param path: Optional path for CWD.

    :return: max date in the folder.
    """
    ftp_files = list_ftp_dir(url, path=path)

    return max([x.date for x in ftp_files])


def latest_date_version_ftp_file(url, file_name, path=None):
    """
    Get the date when the file was last changed.

    :param url: source URL
    :type url: str
    :param file_name: the name of the file to check
    :type file_name: str
    :param path: optional path to the file's folder.
    :type path: str

    :return: the last update date of the file.
    :rtype: str
    """

    ftp_files = list_ftp_dir(url, path)
    for file in ftp_files:
        if file.name == file_name:
            return file.date
    else:
        raise FileNotFoundError("File is not present in the folder. Check the ftp folder structure")


def latest_date_in_ftp_dir(url, path=None):
    """
    Get the raw output of FTP LIST on an FTP path, parses the modification date of each file and returns latest date

    :param url: FTP URL for login
    :param path: Optional path on FTP Server for CWD.
    :return: Raw directory list.
    """
    log.debug("List '{0}', optional path '{1}'".format(url, path))

    if not url.startswith("ftp://"):
        url = 'ftp://' + url

    ftp_url = urlparse(url)
    ftp = FTP(ftp_url.netloc)
    ftp.login()

    # move into path of url
    if ftp_url.path:
        ftp.cwd(ftp_url.path)

    # additional CWD if a path is given separately
    if path:
        ftp.cwd(path)
    filelist = []
    ftp.retrlines('NLST', callback=filelist.append)

    date = datetime.date(year=datetime.MINYEAR, month=1, day=1)

    year = 0
    month = 0
    day = 0

    for filename in filelist:
        datetimeftp = ftp.sendcmd('MDTM ' + filename)
        datetimeftp = datetimeftp.split()[1]
        year = int(datetimeftp[0:4])
        month = int(datetimeftp[4:6])
        day = int(datetimeftp[6:8])
        date = max(date, datetime.date(year, month, day))

    return date


##############################################################
# HTTP helper functions
##############################################################
def get_latest_date_http_file(url):
    """
    Requests HEAD for the file, extracts the date from "Last-Modified" if possible.
    If "Last-Modified" is not in the header, returns today.

    :param url: full url to file
    :return: datetime.date(year-month-day)
    """
    response = requests.head(url, allow_redirects=True)
    if response.status_code == 200:
        return parser.parse(response.headers.get("Last-Modified", str(datetime.date.today()))).date()
    else:
        raise FileNotFoundError("Failed with the http status code {}".format(response.status_code))


def get_links(content):
    soup = BeautifulSoup(content)
    for a in soup.findAll('a'):
        yield a.get('href')