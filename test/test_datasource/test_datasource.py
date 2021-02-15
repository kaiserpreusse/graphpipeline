import pytest

from graphpipeline.datasource import SingleVersionRemoteDataSource, RollingReleaseRemoteDataSource, \
    ManyVersionsRemoteDataSource
from graphpipeline.datasource.datasource import RemoteDataSource


def test_instances(tmp_path):
    SingleVersionRemoteDataSource(root_dir=tmp_path)
    RollingReleaseRemoteDataSource(root_dir=tmp_path)
    ManyVersionsRemoteDataSource(root_dir=tmp_path)


class TestCheckAvailable:

    def test_http_not_existing(self, tmp_path):
        class TestDataSource(RemoteDataSource):
            def __init__(self, root_dir):
                super(TestDataSource, self).__init__(root_dir)

                self.remote_files = ['http://does.not.exist', 'ht//does.not.exist']

        ds = TestDataSource(tmp_path)
        result = ds.check_remote_files()
        assert len(result['found']) == 0
        assert result['not_found'] == ds.remote_files

    def test_http_existing(self, tmp_path):
        class TestDataSource(RemoteDataSource):
            def __init__(self, root_dir):
                super(TestDataSource, self).__init__(root_dir)

                self.remote_files = ['https://google.com',]

        ds = TestDataSource(tmp_path)
        result = ds.check_remote_files()
        assert result['found'] == ds.remote_files

    def test_ftp_existing(self, tmp_path):

        class TestDataSource(RemoteDataSource):
            def __init__(self, root_dir):
                super(TestDataSource, self).__init__(root_dir)

                self.remote_files = ['ftp://ftp.ensembl.org/pub/release-100/gtf/homo_sapiens',
                                     'ftp://ftp.ensembl.org/pub/release-100/gtf/homo_sapiens/']

        ds = TestDataSource(tmp_path)
        result = ds.check_remote_files()
        assert result['found'] == ds.remote_files

    def test_ftp_not_existing(self, tmp_path):
        class TestDataSource(RemoteDataSource):
            def __init__(self, root_dir):
                super(TestDataSource, self).__init__(root_dir)

                self.remote_files = ['ftp://ftp.ensembl.org/pub/wring/path/release-100/gtf/homo_sapiens']

        ds = TestDataSource(tmp_path)
        result = ds.check_remote_files()
        assert result['not_found'] == ds.remote_files