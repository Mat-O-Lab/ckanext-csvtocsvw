import io
import locale
import logging
from collections import OrderedDict
from typing import List, Tuple
from urllib.parse import unquote, urlparse
from urllib.request import urlopen

import pandas as pd
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.collection import Collection
from rdflib.namespace import CSVW, RDF, XSD
from rdflib.util import guess_format

locale.setlocale(locale.LC_ALL, "en_US.UTF-8")

QUDT_UNIT_URL = "./ontologies/qudt_unit.ttl"
QUDT = Namespace("http://qudt.org/schema/qudt/")
QUNIT = Namespace("http://qudt.org/vocab/unit/")
OA = Namespace("http://www.w3.org/ns/oa#")
XSD_NUMERIC = [XSD.float, XSD.decimal, XSD.integer, XSD.double]

DataStoreTypes = {
    XSD.string: "text",
    XSD.dateTime: "timestamp",
    XSD.boolean: "bool",
    XSD.integer: "int",
    XSD.double: "float",
    XSD.anyURI: "text",
    XSD.date: "date",
}
# using  bject for floats because of german notation with ,
PandasTypes = {
    XSD.string: "object",
    XSD.dateTime: "object",
    XSD.boolean: "boolean",
    XSD.integer: "Int32",
    # XSD.double: "Float64",
    XSD.double: "object",
    XSD.anyURI: "object",
    XSD.date: "object",
}


def get_columns_from_schema(schema: URIRef, graph: Graph) -> OrderedDict:
    """_summary_

    Args:
        schema (URIRef): csvw.TableSchema to get columns objects from
        graph (Graph): Graph which includes the TableSchema

    Returns:
        OrderedDict: Dictionary with all Clumn informations attacked
    """
    column_collection_node = next(graph[schema : CSVW.column :], None)
    # collection must be queried in another way
    column_collection = Collection(graph, column_collection_node)
    columns = list(column_collection)
    return OrderedDict(
        [(column, {k: v for (k, v) in graph[column:]}) for column in columns]
    )


def simple_columns(columns: OrderedDict) -> list:
    col_dict = OrderedDict(
        [
            (
                key,
                {"id": str(value[CSVW.name]), "info": {"label": str(value[CSVW.name])}},
            )
            for key, value in columns.items()
            if str(value[CSVW.name]) != "GID"
        ]
    )
    for key, value in col_dict.items():
        if CSVW.format in columns[key].keys():
            col_dict[key]["type"] = DataStoreTypes.get(
                columns[key][CSVW.format], "text"
            )
            col_dict[key]["pandas_type"] = PandasTypes.get(
                columns[key][CSVW.format], "object"
            )
        if QUDT.unit in columns[key].keys():
            col_dict[key]["info"]["notes"] = "Unit: " + str(columns[key][QUDT.unit])
    return [value for key, value in col_dict.items()]


def parse_csv_from_url_to_list(
    csv_data,
    columns: dict,
    delimiter: str = ",",
    skiprows: int = 0,
    num_header_rows: int = 2,
    encoding: str = "utf-8",
) -> List[List]:
    """Parses a csv file using the dialect given, to a list containing the content of every row as list.

    Args:
        csv_url (_type_): Url to the csv file to parse
        delimiter (str, optional): Delimiter for columns. Defaults to ','.
        skiprows (int, optional): Rows to Skip reading. Defaults to 0.
        num_header_rows (int, optional): Number of header rows with names of columns. Defaults to 2.
        encoding (str, optional): Encoding of the csv file. Defaults to 'utf-8'.

    Returns:
        List[List]: List of Lists with entrys for each row. Content of header rows are not included
    """
    file_data = csv_data
    file_string = io.StringIO(file_data.decode(encoding))
    num_cols = len(columns) - 1
    column_names = [column["id"] for column in columns]
    types = {column["id"]: column["pandas_type"] for column in columns}
    # print(delimiter,num_header_rows+skiprows)
    table_data = pd.read_csv(
        file_string,
        header=None,
        sep=delimiter,
        dtype=types,
        names=column_names,
        # usecols=column_names,
        skiprows=num_header_rows + skiprows,
        encoding=encoding,
        skip_blank_lines=False,
        # on_bad_lines=test_bad_line,
        engine="python",
    )
    # remove data after blank line
    logging.debug(table_data.dtypes)
    blank_df = table_data.loc[table_data.isnull().all(1)]
    if len(blank_df) > 0:
        first_blank_index = blank_df.index[0]
        table_data = table_data[:first_blank_index]
    # replace decimal seperator in numeric data concerning locale - use us_uk notation with .
    # table_data.map(lambda x: locale.atof(str(x)) if ',' in str(x) else str(x),inplace=True)
    table_data = table_data.map(
        lambda x: (
            str(x).replace(",", ".").replace(".", "", str(x).count("."))
            if "," in str(x)
            else str(x)
        )
    )

    # replace nan values with empty strings to be serializable as json
    table_data.fillna("", inplace=True)
    # add a row index column
    line_list = table_data.to_numpy().tolist()
    line_list = [
        [
            index,
        ]
        + line
        for index, line in enumerate(line_list)
    ]
    return line_list


def open_csv(uri: str) -> Tuple[str, str]:
    """Open a csv file for reading, returns filedata and filename in a Tuple.

    Args:
        uri (str): Uri to the file to read

    Returns:
        Tuple[str,str]: Tuple of filedata and filename
    """
    print("try to open: {}".format(uri))
    try:
        uri_parsed = urlparse(uri)
    except:
        print("not an uri - if local file add file:// as prefix")
        return None
    else:
        filename = unquote(uri_parsed.path).rsplit("/download/upload")[0].split("/")[-1]
        if uri_parsed.scheme in ["https", "http"]:
            filedata = urlopen(uri).read()

        elif uri_parsed.scheme == "file":
            filedata = open(unquote(uri_parsed.path), "rb").read()
        else:
            print("unknown scheme {}".format(uri_parsed.scheme))
            return None
        return filedata, filename


def parse_graph(url: str, graph: Graph, format: str = "") -> Graph:
    """Parse a Graph from web url to rdflib graph object
    Args:
        url (AnyUrl): Url to an web ressource
        graph (Graph): Existing Rdflib Graph object to parse data to.
    Returns:
        Graph: Rdflib graph Object
    """
    logging.debug("parsing graph from {}".format(url))
    parsed_url = urlparse(url)
    META = Namespace(url + "/")
    # print(parsed_url)
    if not format:
        format = guess_format(parsed_url.path)
    if parsed_url.scheme in ["https", "http"]:
        graph.parse(urlopen(parsed_url.geturl()).read(), format=format)

    elif parsed_url.scheme == "file":
        print(parsed_url.path)
        graph.parse(parsed_url.path, format=format)
    graph.bind("meta", META)

    print(parsed_url)
    return graph


class CSVWtoRDF:
    """Class for Converting CSV data to RDF with help of CSVW Annotations"""

    def __init__(self, metadata: str, csv: str, metaformat: str = "json-ld") -> None:
        # self.metadata_url=str(metadata_url)
        # get metadata graph
        # self.metagraph=parse_graph(self.metadata_url,Graph(),format=metaformat)
        self.metagraph = Graph().parse(data=metadata, format=metaformat)
        # self.metagraph.serialize('test.ttl',format='turtle')
        # print(list(self.metagraph[: CSVW.url]))
        if len(list(self.metagraph[: CSVW.url])) == 0:
            # if no url then also no table to parse
            self.meta_root = ""
            self.base_url = ""
            self.tables = {}
            return None
        else:
            self.meta_root, url = list(self.metagraph[: CSVW.url])[0]
            # self.metagraph.serialize('metagraph.ttl')
            # print('meta_root: '+self.meta_root)
            # print('csv_url: '+url)
            self.base_url = "{}/".format(
                str(self.meta_root).rsplit("/download/upload")[0].rsplit("/", 1)[0]
            )
            parsed_url = urlparse(url)
            if parsed_url.scheme in ["https", "http", "file"]:
                self.csv_url = url
            else:
                self.csv_url = self.base_url + url
            self.graph = Graph(base=self.csv_url + "/")
            self.filename = self.csv_url.rsplit("/", 1)[-1].rsplit(".", 1)[0]
            self.tables = {
                table_node: {} for file, table_node in self.metagraph[: CSVW.table :]
            }
            print("tables: {}".format(self.tables))
            self.table_data = list()
            if self.tables:
                for key, data in self.tables.items():
                    dialect = next(self.metagraph[key : CSVW.dialect], None)
                    data["dialect"] = {
                        k: v.value for (k, v) in self.metagraph[dialect:]
                    }
                    # print(data['dialect'])
                    data["schema"] = next(
                        self.metagraph[key : CSVW.tableSchema :], None
                    )
                    data["columns"] = get_columns_from_schema(
                        data["schema"], self.metagraph
                    )
                    # print(data['columns'])
                    # get table form csv_url
                    if data["schema"]:
                        data["about_url"] = next(
                            self.metagraph[data["schema"] : CSVW.aboutUrl], None
                        )
                        # print(data['dialect'])
                        print(
                            "skipRows: {} headerRowCount: {}".format(
                                data["dialect"][CSVW.skipRows],
                                data["dialect"][CSVW.headerRowCount],
                            )
                        )
                        data["lines"] = parse_csv_from_url_to_list(
                            csv,
                            delimiter=data["dialect"][CSVW.delimiter],
                            skiprows=data["dialect"][CSVW.skipRows],
                            # always on column less, index is created after reading
                            columns=simple_columns(data["columns"]),
                            num_header_rows=data["dialect"][CSVW.headerRowCount],
                            encoding=data["dialect"][CSVW.encoding],
                        )

    def add_table_data(self, g: Graph) -> Graph:
        """_summary_

        Args:
            g (Graph): Grapg to add the table data tripells to

        Returns:
            Graph: Input Graph return with triples of table added
        """
        for table, data in self.tables.items():
            print("table: {}, about_url: {}".format(table, data["about_url"]))
            # g.add((table_group,CSVW.table, table))
            g.add((table, RDF.type, CSVW.Table))
            if data["about_url"]:
                row_uri = data["about_url"]
            else:
                row_uri = "table-{TABLE}-gid-{GID}".format(table)
            columns = list(data["columns"].items())
            for index, row in enumerate(data["lines"]):
                # print(index, row)
                row_node = BNode()
                values_node = URIRef(row_uri.format(GID=index))
                g.add((table, CSVW.row, row_node))
                g.add((row_node, RDF.type, CSVW.Row))
                g.add((row_node, CSVW.describes, values_node))
                row_num = (
                    index
                    + data["dialect"][CSVW.skipRows]
                    + data["dialect"][CSVW.headerRowCount]
                )
                g.add(
                    (
                        row_node,
                        CSVW.url,
                        URIRef("{}/row={}".format(self.csv_url, row_num)),
                    )
                )
                for cell_index, cell in enumerate(row):
                    # print(self.columns[cell_index])
                    column_data = columns[cell_index][1]
                    if column_data[CSVW.name] == Literal("GID"):
                        continue
                    format = column_data.get(CSVW.format, XSD.string)
                    unit = column_data.get(QUDT.unit, None)
                    if format == XSD.double and isinstance(cell, str):
                        cell = cell.replace(".", "")
                        cell = cell[::-1].replace(",", ".", 1)[::-1]

                    if format in XSD_NUMERIC:
                        value_node = BNode()
                        g.add((value_node, RDF.type, QUDT.QuantityValue))
                        g.add((value_node, QUDT.value, Literal(cell)))
                        if unit:
                            g.add((value_node, QUDT.unit, unit))
                    elif format == XSD.anyURI:
                        # see if its a list of uris
                        if len(cell.split(" ")) >= 1:
                            value_node = BNode()
                            uris = list(map(URIRef, cell.split(" ")))
                            Collection(g, value_node, uris)
                        else:
                            value_node = URIRef(cell)
                    else:
                        value_node = BNode()
                        body_node = BNode()
                        g.add((value_node, RDF.type, OA.Annotation))
                        g.add((value_node, OA.hasBody, body_node))
                        g.add((body_node, RDF.type, OA.TextualBody))
                        g.add((body_node, OA["format"], Literal("text/plain")))
                        g.add((body_node, OA.value, Literal(cell, datatype=format)))

                    # if isinstance(column,URIRef) and str(self.meta_root)!='file:///src/': #has proper uri
                    #     g.add((value_node, column, Literal(cell)))

                    if CSVW.aboutUrl in column_data.keys():
                        aboutUrl = column_data[CSVW.aboutUrl]
                        g.add(
                            (
                                values_node,
                                URIRef(aboutUrl.format(GID=index)),
                                value_node,
                            )
                        )
                    else:
                        name = column_data[CSVW.name]
                        g.add((values_node, URIRef(name), value_node))
        return g
        # self.atdm, self.metadata =converter.convert_to_atdm('standard')
