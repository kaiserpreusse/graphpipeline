import pytest

from graphpipeline.datasource import SingleVersionRemoteDataSource, RollingReleaseRemoteDataSource, \
    ManyVersionsRemoteDataSource


def test_instances(tmp_path):
    SingleVersionRemoteDataSource(root_dir=tmp_path)
    RollingReleaseRemoteDataSource(root_dir=tmp_path)
    ManyVersionsRemoteDataSource(root_dir=tmp_path)
