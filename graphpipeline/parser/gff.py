import gzip
from collections import namedtuple


class GffReader(object):
    """
    Read GFF3 and GTF files and return a set of Records.
    """

    def __init__(self, gff_file_path, file_type=None):
        self.gff_file_path = gff_file_path

        # determine file type (gtf/gff) from file name if not supplied
        if not file_type:
            if '.gtf' in gff_file_path:
                self.file_type = 'gtf'
            elif '.gff' in gff_file_path:
                self.file_type = 'gff'
            else:
                raise IOError("Cannot guess file type from filename: %s" % gff_file_path)
        else:
            if file_type in ['gff', 'gtf']:
                self.file_type = file_type
            else:
                raise AttributeError("file_type must be 'gtf' or 'gff'")

    @property
    def records(self):
        return self.read_file()

    def read_file(self):
        """
        :return: Record namedtuple
        """
        Record = namedtuple('Record',
                            ['chr', 'source', 'type', 'start', 'end', 'score', 'strand', 'frame', 'attributes'])

        if self.gff_file_path.endswith('.gz'):
            f = gzip.open(self.gff_file_path, 'rt')
        else:
            f = open(self.gff_file_path, 'rt')

        for l in f:
            if not l.startswith('#'):
                flds = l.split('\t')

                # get attribtues and parse depending on type
                # gff files contain "attribute=some_value; another=some_value"
                # gtf files contain "gene_id "ENSG00000223972"; gene_name "DDX11L1";"
                attributes = None
                if self.file_type == 'gff':
                    attributes = self.read_gff_attributes(flds[8])
                elif self.file_type == 'gtf':
                    attributes = self.read_gtf_attributes(flds[8])

                yield Record(flds[0], flds[1], flds[2], flds[3], flds[4], flds[5], flds[6], flds[7], attributes)

        f.close()

    @staticmethod
    def read_gff_attributes(attribute_column):
        """
        Parse attributes for a GFF3 record. Attributes with pre-defined meaning are parsed according to their
        specification (e.g. Dbxref usually has multiple values which are split up: 'GeneID:1234,Genbank:NM_9283').


        :param attribute_column: Attribute column of a GFF3 file.
        :return: Dictionary of attributes.
        :rtype: dict
        """
        attributes = {}
        for a in attribute_column.split(';'):
            # there is a leading space for some fields
            a = a.strip()
            # an attribute looks like 'Alias=MIMAT0027693'
            key = a.split('=')[0]
            value = a.split('=')[1]

            # handle pre-defined attributes
            if key == 'Dbxref':
                # a dbxref line looks like: GeneID:1234,Genbank:NM_9283
                dbxref_dict = {}
                for dbxref_entry in value.split(','):
                    dbxref_key, dbxref_value = dbxref_entry.split(':', 1)
                    dbxref_dict[dbxref_key] = dbxref_value
                value = dbxref_dict

            attributes[key] = value

        return attributes

    @staticmethod
    def read_gtf_attributes(attribute_line):
        attributes = {}
        for a in attribute_line.split(';'):
            # there is a leading space for some fields
            a = a.strip()
            try:
                # an attribute looks like 'gene_id "ENSG00000274890"'
                key = a.split()[0].replace('"', '')
                value = a.split()[1].replace('"', '')
                attributes[key] = value
            except IndexError:
                pass

        return attributes
