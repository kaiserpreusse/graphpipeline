import logging
from graphio import Container, NodeSet, RelationshipSet

log = logging.getLogger(__name__)


class Parser:

    def __init__(self, root_dir):

        self.root_dir = root_dir

        self.datasource_instances = []

        self.name = self.__class__.__name__

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


class ReturnParser(Parser):
    """
    Base class for parsers which run over files and store ouput in memory.

    They need an explicit .run() function.
    """
    TYPE = 'return'

    def __init__(self, root_dir):
        super(ReturnParser, self).__init__(root_dir)

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

    def __init__(self, root_dir):
        super(YieldParser, self).__init__(root_dir)
