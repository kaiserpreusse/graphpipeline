from graphpipeline.datasource.datasource import RemoteDataSource
from graphpipeline.datasource import DataSourceInstance


class SomeDataSource(RemoteDataSource):
    def __init__(self, root_dir):
        super(SomeDataSource, self).__init__(root_dir)



def test_datasourceinstance_serialization(tmp_path):

    ds = SomeDataSource(tmp_path)

    datasource_instance = DataSourceInstance(ds)

    dsi_dict = datasource_instance.to_dict()

    reloaded_datasource_instance = DataSourceInstance.from_dict(dsi_dict)

    assert reloaded_datasource_instance.to_dict() == dsi_dict




