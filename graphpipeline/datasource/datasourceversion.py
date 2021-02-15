import os
import six
from distutils.version import LooseVersion
import datetime
import re
import logging

from graphpipeline.datasource.helper import filehandler

log = logging.getLogger(__name__)


class DataSourceVersion(object):

    def __init__(self, version):
        """
        :param version: Version specification.
        """

        # try parsing string
        # if begins with number ('10.3')
        if isinstance(version, six.string_types):
            # catch the datetime.datetime string representations
            if re.match('^[0-9]', version) and '___' in version and '__' in version and '_' in version:
                self.version = filehandler._datetime_from_dirname(version)
            # match version with '-' that are likely not dates (i.e. they do not consist of three fields)
            elif re.match('^[0-9]', version) and len(version.split('-')) != 3:
                self.version = LooseVersion(version)
            # match date like strings (2013-05-06)
            elif re.match('^[0-9]', version) and len(version.split('-')) == 3:
                date_flds = version.split('-')
                self.version = datetime.date(int(date_flds[0]), int(date_flds[1]), int(date_flds[2]))
            else:
                try:
                    self.version = LooseVersion(version)
                except TypeError:
                    self.version = version

        # integer versions are translated into LooseVersion to be compatible with getting versions from
        # e.g. file lists which might produce a string
        elif isinstance(version, int):
            self.version = LooseVersion(str(version))

        # datetime.datetime is retained for now
        elif isinstance(version, datetime.datetime):
            self.version = version

        else:
            self.version = version

        self._version_type = type(self.version)

        # set the dirname depending on the type of version
        self.dir_repr = None
        if isinstance(self.version, LooseVersion):
            self.dir_repr = str(self.version)
        elif isinstance(self.version, datetime.date) and not isinstance(self.version, datetime.datetime):
            self.dir_repr = str(self.version)
        elif isinstance(self.version, datetime.datetime):
            self.dir_repr = filehandler._dirname_from_datetime(self.version)
        else:
            self.dir_repr = str(self.version)

    def __eq__(self, other):
        try:
            return self.version == other.version
        except TypeError:
            log.warning("Trying to compare versions with different scheme: {0} and {1}".format(
                self.version, other.version
            ))

    def __ne__(self, other):
        try:
            return self.version != other.version
        except TypeError:
            log.warning("Trying to compare versions with different scheme: {0} and {1}".format(
                self.version, other.version
            ))

    def __lt__(self, other):
        """
        Implement 'less than' comparison for all used data types.
        """
        try:
            return self.version < other.version
        except TypeError:
            log.warning("Trying to compare versions with different scheme: {0} and {1}".format(
                self.version, other.version
            ))

    def __le__(self, other):
        """
        Implement 'less than or equal' comparison for all used data types.
        """
        try:
            return self.version <= other.version
        except TypeError:
            log.warning("Trying to compare versions with different scheme: {0} and {1}".format(
                self.version, other.version
            ))

    def __gt__(self, other):
        """
        Implement 'greater than' comparison for all used data types.
        """
        try:
            return self.version > other.version
        except TypeError:
            log.warning("Trying to compare versions with different scheme: {0} and {1}".format(
                self.version, other.version
            ))

    def __ge__(self, other):
        """
        Implement 'greater than or equal' comparison for all used data types.
        """
        try:
            return self.version >= other.version
        except TypeError:
            log.warning("Trying to compare versions with different scheme: {0} and {1}".format(
                self.version, other.version
            ))

    def __str__(self):
        return str(self.version)

    def __repr__(self):
        return repr(self.version)

    @classmethod
    def version_from_string(cls, string):
        return cls(string)

    @classmethod
    def versions_from_dir(cls, dirname):
        exclude = ['.DS_Store']

        version_list = []

        for x in os.listdir(dirname):
            if x and x not in exclude:
                version_list.append(
                    cls(x)
                )

        return version_list
