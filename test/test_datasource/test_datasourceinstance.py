from graphpipeline.datasource.datasource import RemoteDataSource
from graphpipeline.datasource import DataSourceInstance


class SomeDataSource(RemoteDataSource):
    def __init__(self, root_dir):
        super(SomeDataSource, self).__init__(root_dir)



def test_datasourceinstance_serialization(tmp_path):

    ds = SomeDataSource(tmp_path)

    datasource_instance = DataSourceInstance(ds)

    dsi_dict = datasource_instance.to_dict()

    reloaded_datasource_instance = DataSourceInstance.from_dict(dsi_dict, root_dir=tmp_path)

    assert reloaded_datasource_instance.to_dict() == dsi_dict




# import pytest
# import os
# from collections import namedtuple
#
# from biomedgraph.datasources.datasourceinstance import DataSourceInstance
#
# def test_find_files_in_directory(tmp_path):
#     DataSource = namedtuple('DataSource', ['ds_dir'])
#     test_datasource = DataSource(tmp_path)
#
#     dsi = DataSourceInstance(test_datasource)
#
#     # create stuff in instance dir
#     os.makedirs(os.path.join(dsi.instance_dir, 'mouse'))
#     os.makedirs(os.path.join(dsi.instance_dir, 'human'))
#
#     with open(os.path.join(dsi.instance_dir, 'mouse', 'data.txt'), 'a'):
#         os.utime(os.path.join(dsi.instance_dir, 'mouse', 'data.txt'), None)
#
#     with open(os.path.join(dsi.instance_dir, 'human', 'data.txt'), 'a'):
#         os.utime(os.path.join(dsi.instance_dir, 'human', 'data.txt'), None)
#
#     get_file = dsi.get_file_from_directory('human', 'data.txt')
#     assert 'human' in get_file
#     assert 'mouse' not in get_file