import importlib
import logging
import os
import json
from typing import List
from datetime import date, datetime

from graphio import Container, NodeSet, RelationshipSet
from py2neo import Graph

from graphpipeline.datasource import DataSourceInstance

log = logging.getLogger(__name__)


def run_parser_merge_nodes(graph_config: tuple, parser_class_name: str, import_path: str, parser_arguments: dict, datasourceinstances: List[dict], root_dir: str):
    """
    Run a parser in a Pool/RPC.

    :param parser_class_name: Name of the parser class.
    :param import_path: Path where to import from.
    :param parser_arguments: Arguments for the parser.
    :param datasourceinstance_dict: serialized DataSourceInstance
    """

    graph = Graph(graph_config[0], name=graph_config[1])

    module = importlib.import_module(import_path)
    parser_class = getattr(module, parser_class_name)

    parser = parser_class()
    # add datasource instances
    for dsi_dict in datasourceinstances:
        dsi = DataSourceInstance.from_dict(dsi_dict, root_dir)
        parser.datasource_instances.append(dsi)
    # add arguments
    for k, v in parser_arguments.items():
        parser.__dict__[k] = v

    parser.run_with_mounted_arguments()

    for ns in parser.container.nodesets:
        ns.create_index(graph)
        ns.merge(graph)


def run_and_serialize(target_dir: str, parser_class_name: str, import_path: str, parser_arguments: dict, datasourceinstances: List[dict], root_dir: str):
    """
    Run the parser and serialize the output to a target directory.

    :param target_dir:
    :param parser_class_name:
    :param import_path:
    :param parser_arguments:
    :param datasourceinstances:
    :param root_dir:
    :return:
    """
    log.debug(f"Run {parser_class_name} with {parser_arguments}")
    module = importlib.import_module(import_path)
    parser_class = getattr(module, parser_class_name)

    parser = parser_class()
    # add datasource instances
    for dsi_dict in datasourceinstances:
        dsi = DataSourceInstance.from_dict(dsi_dict, root_dir)
        parser.datasource_instances.append(dsi)
    # add arguments
    for k, v in parser_arguments.items():
        parser.__dict__[k] = v

    parser.run_with_mounted_arguments()

    parser.serialize(target_dir)


class Parser:

    def __init__(self):

        self.datasource_instances = []

        self.name = self.__class__.__name__
        self.arguments = []

    def get_arguments(self) -> dict:
        """
        Return a dictionary of Parser run arguments.
        """
        parser_arguments = {}
        for k in self.arguments:
            parser_arguments[k] = self.__dict__[k]
        return parser_arguments

    def get_instance_by_name(self, name):
        """

        :param name:
        :return: The DataSourceInstance
        :rtype: DataSourceInstance
        """
        for datasource_instance in self.datasource_instances:
            if datasource_instance.datasource.name == name:
                return datasource_instance

    def get_nodeset(self, labels, merge_keys):
        return self.container.get_nodeset(labels, merge_keys)

    @property
    def container(self):
        container = Container()
        for k, o in self.__dict__.items():
            if isinstance(o, NodeSet) or isinstance(o, RelationshipSet):
                container.add(o)
        return container

    def merge(self, graph):
        for nodeset in self.container.nodesets:
            nodeset.create_index(graph)
            nodeset.merge(graph)
        for relset in self.container.relationshipsets:
            relset.create_index(graph)
            relset.merge(graph)

    def create(self, graph):
        for nodeset in self.container.nodesets:
            nodeset.create_index(graph)
            nodeset.create(graph)
        for relset in self.container.relationshipsets:
            relset.create_index(graph)
            relset.create(graph)

    def run_with_mounted_arguments(self):
        """
        Unified interface_dev to run the parser. Does not take parameters (all necessary
        arguments must be stored in the Parser class).

        Returns the output of the parser function.
        """
        raise NotImplementedError

    def run(self, *args, **kwargs):
        """
        Run the parser with specific parameters.

        Returns the output of the parser function.
        """
        raise NotImplementedError

    def _serialization_dir_name(self) -> str:
        """
        Create a name that identifies the Parser.

            MyParser_taxid_9606

        :return: Parser name.
        """

        parser_name = f"{self.__class__.__name__}"
        if self.arguments:
            for k, v in self.get_arguments().items():
                parser_name += f"_{k}_{v}"
        return parser_name

    def metadata_dict(self) -> dict:
        """
        Create dictionary of Parser metadata, do not include the NodeSets and RelationshipSets.
        """
        output = dict(
            name=self.name,
            datasource_instances=[dsi.to_dict() for dsi in self.datasource_instances]
        )
        for a in self.arguments:
            output[a] = self.__dict__[a]

        return output

    def serialize(self, target_dir: str, overwrite: bool = True):
        """
        Store the Parser with output in a directory.

        Default behaviour is to delete existing nodeset/relationship set files in the target directory.
        """

        # serializer for datetime
        serialization_dir_name = self._serialization_dir_name()
        log.debug(f"Serialize {self.__class__.__name__} to {target_dir}/{serialization_dir_name}. Overwrite is {overwrite}.")

        def json_serial(obj):
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            raise TypeError("Type %s not serializable" % type(obj))

        output_dir = os.path.join(target_dir, serialization_dir_name)
        # clean output directory
        if overwrite:
            if os.path.exists(output_dir):
                for f in os.listdir(output_dir):
                    if f.startswith('nodeset_') and f.endswith('.json'):
                        os.remove(os.path.join(output_dir, f))
                    if f.startswith('relationshipset_') and f.endswith('.json'):
                        os.remove(os.path.join(output_dir, f))
                    if f == 'parser_data.json':
                        os.remove(os.path.join(output_dir, f))

        if not os.path.exists(output_dir):
            os.mkdir(output_dir)

        metadate_path = os.path.join(output_dir, 'parser_data.json')
        with open(metadate_path, 'wt') as f:
            json.dump(self.metadata_dict(), f, default=json_serial)

        for nodeset in self.container.nodesets:
            nodeset.serialize(output_dir)

        for relset in self.container.relationshipsets:
            relset.serialize(output_dir)

    @classmethod
    def deserialize(cls, source_dir: str, metadata_only: bool = False) -> 'Parser':
        """
        Read from a serialized directory, recreate a Parser that can load to the database.

        :param source_dir: Directory to read from.
        :return: A Parser object.
        """
        log.debug(f"Read Parser from {source_dir}.")
        p = cls()

        for file in os.listdir(source_dir):
            if not metadata_only:
                if file.startswith('nodeset_'):
                    ns_name = file.replace('.json', '')
                    with open(os.path.join(source_dir, file), 'rt') as f:
                        log.debug(f"Deserialize {f}")
                        ns = NodeSet.from_dict(json.load(f))
                        log.debug(f"Num nodes in NodeSet: {len(ns.nodes)}")
                        p.__dict__[ns_name] = ns

                elif file.startswith('relationshipset_'):
                    rs_name = file.replace('.json', '')
                    with open(os.path.join(source_dir, file), 'rt') as f:
                        log.debug(f"Deserialize {f}")
                        rs = RelationshipSet.from_dict(json.load(f))
                        log.debug(f"Num relationships in RelationshipSet: {len(rs.relationships)}")
                        p.__dict__[rs_name] = rs

            if file == 'parser_data.json':
                with open(os.path.join(source_dir, file), 'rt') as f:
                    metadata = json.load(f)
                    # TODO add datasource instances to deserializer
                    p.name = metadata['name']

        return p


class ReturnParser(Parser):
    """
    Base class for parsers which run over files and store ouput in memory.

    They need an explicit .run() function.
    """
    TYPE = 'return'

    def __init__(self):
        super(ReturnParser, self).__init__()

    @property
    def container(self):
        container = Container()
        for k, o in self.__dict__.items():
            if isinstance(o, NodeSet) or isinstance(o, RelationshipSet):
                container.add(o)
        return container

    def _reset_parser(self):
        """
        Delete all NodeSets and RelationshipSets to free up memory in data loading pipelines.
        """
        log.info("Reset Parser {}".format(self.__class__.__name__))

        for k, o in self.__dict__.items():
            if isinstance(o, NodeSet):
                log.info("Delete nodes for NodeSet with key {}".format(k))
                del o.nodes
                o.nodes = []
            elif isinstance(o, RelationshipSet):
                log.info("Delete relationships for RelationshipSet with key {}".format(k))
                del o.relationships
                o.relationships = []


class YieldParser(Parser):
    """
    Base class for parsers which mount yield functions into the ObjectSets.

    Used for very large data sets which do not fit into memory.
    """
    TYPE = 'yield'

    def __init__(self):
        super(YieldParser, self).__init__()
