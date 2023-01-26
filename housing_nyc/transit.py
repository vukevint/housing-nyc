"""
Classes and functions for ETL of graph data.

TODO:
1. Create generic graph class, and subclass it for walk, gtfs, and integrated (see urbanaccess).
"""

import urbanaccess as ua
import pandas as pd
import osmnx as ox
import networkx as nx
import itertools
import time
from numpy import array as array_

from .utils.globals import today
from .utils.paths import gtfs_feed_path


def bbox_ox(bbox):
    lng_max, lat_min, lng_min, lat_max = bbox
    return (lat_max, lat_min, lng_min, lng_max)


class WalkGraph:
    """
    Generates nodes and edges of NYC walk graph from OpenStreetMap. Can read saved network graphs from graphml, and 
    restore most of the original OSM graph (missing some attributes like G.graph).

    TODO:

    Parameters
    ----------

    Returns
    -------
    edges : geopandas.DataFrame
    nodes : geopandas.DataFrame
    """
    def __init__(self, network_type='walk', simplify=False):
        self._network_type = network_type
        self._simplify = simplify
        self.nodes, self.edges = self.download_graph()

        graph_attr = {'crs': 'epsg:4326'}

    def download_graph(self):
        # staten island is not included in 'New York City', but not sure if this is true for non-walk network types
        self._G = ox.graph_from_place('New York City', network_type=self._network_type, simplify=self._simplify)
        self._G_si = ox.graph_from_place('Staten Island', network_type=self._network_type, simplify=self._simplify)

        gdf = ox.graph_to_gdfs(self._G)
        gdf_si = ox.graph_to_gdfs(self._G_si)

        nodes = pd.concat([gdf[0], gdf_si[0]])
        edges = pd.concat([gdf[1], gdf_si[1]])

        return nodes, edges

    def save_graph(self, path):
        C = nx.compose(self.G, self.G_si)
        ox.save_graphml(C, path)


class GtfsGraph:
    """
    Generates nodes and edges from GTFS feed. Builds on urbanaccess to load and format the feed
    as pandas dataframes.

    Contains methods to restructure the dataframes to include transfer edges, and average the weights by
    various preset options.

    TODO:
    - make the graph compatible with urbanaccess network integration with osm
    - create log saving capabilities
    - allow imports of csv files that were exported from this class by save_graph. should account for column name
    change
    - add 'properties' (attributes?) to class that denotes if the edges are averaged, or modified in any way using
    methods

    Parameters
    ----------
    feed_path : str
        Path to folder of gtfs feed containing routes.txt, stop_times.txt, etc.
    day : {'friday', 'monday', 'saturday', 'sunday', 'thursday', 'tuesday', 'wednesday'}
        Day of the week to extract transit schedule from that corresponds to the day in the GTFS calendar.
    timerange : list
        Time range to extract transit schedule from in a list with time 1 and time 2. It is suggested
        the time range specified is large enough to allow for travel from one end of the transit
        network to the other but small enough to represent a relevant travel time period such as
        a 3 hour window for the AM Peak period. Must follow format of a 24 hour clock for example:
        08:00:00 or 17:00:00

    Returns
    -------
    edges : pandas.DataFrame
    nodes : pandas.DataFrame
    transfer_edges : pandas.DataFrame
    """

    def __init__(self, day=None, timerange=None):
        if day is None:
            self.day = "monday"  # weekday schedule
            print("Setting calendar day to 'monday' to extract from feed.")
        else:
            self.day = day

        if timerange is None:
            self.timerange = ["06:00:00", "10:00:00"]  # rush hour
            print("Setting calendar timerange to rush hours to extract from feed.")
        else:
            self.timerange = timerange

        feed_path = gtfs_feed_path

        # import and process feed with urbanaccess
        _feed = ua.gtfs.load.gtfsfeed_to_df(gtfsfeed_path=feed_path)
        _ua_net = ua.gtfs.network.create_transit_net(
            gtfsfeeds_dfs=_feed,
            day=self.day,
            timerange=self.timerange,
            calendar_dates_lookup=None,
        )
        self.edges = _ua_net.transit_edges
        self.nodes = _ua_net.transit_nodes
        self.expected_wait_times = None
        self.transfer_edges = pd.DataFrame()
        self.averaged_edges = pd.DataFrame()

        # might want to specify agency for graph saving purposes, although for one city, probably doesn't matter since
        # buses and subway may contain the same agency
        # if len(set(self.nodes['unique_agency_id'])) == 1:
        #     self.agency = self.nodes['unique_agency_id'].iloc[0]
        # else:
        #     print("The GTFS feed contains different agency ids. May cause issues when building transfer edges "
        #           "with _permutated_edges. Also filename issues with save_graph.")

        ua.config.settings.log_file = False  # don't save urbanacess log

    def _rename_edges(self):
        """Rewrite edge map with renamed nodes based on unique_agency_id and unique_route_id."""
        new_edges = self.edges
        new_edges["node_id_from"] = new_edges.apply(
            lambda x: x["node_id_from"].replace(x["unique_agency_id"], x["unique_route_id"]), axis=1)
        new_edges["node_id_to"] = new_edges.apply(
            lambda x: x["node_id_to"].replace(x["unique_agency_id"], x["unique_route_id"]), axis=1)
        print("Sucessfully rewrote edges map.")
        return

    def _rename_nodes(self):
        """Rewrite node map with renamed nodes from renamed edges."""
        # initialize variables for new node map
        edges = self.edges
        new_nodes = self.nodes.copy()

        # generate new nodes dataframe using unique list of nodes from renamed edges
        new_node_id = list(set(edges["node_id_from"])) + list(
            set(edges["node_id_to"]) - set(edges["node_id_from"]))
        new_nodes = pd.DataFrame(columns=new_nodes.columns, index=new_node_id)
        new_nodes = new_nodes.rename_axis("node_id")

        # populate new nodes dataframe with appropriate information from old nodes dataframe
        old_node_id = self.nodes.index
        for i in range(len(new_nodes)):
            str_split = new_nodes.iloc[i].name.split("_", 2)
            str_idx = str_split[0] + "_" + str_split[-1]
            idx = old_node_id.get_loc(str_idx)
            new_nodes.iloc[i] = self.nodes.iloc[idx]

        self.nodes = new_nodes
        print("Sucessfully rewrote nodes map.")
        return

    def restructure_graph(self):
        """
        Main method to restructure edges and nodes based on a more specific naming convention. The nodes will
        contain information about the child station, unique route (e.g. train line) that runs through it, and
        the agency that operates the line.
        """
        start_time = time.time()

        # Note: order currently matters since nodes are renamed through edges map
        self._rename_edges()
        self._rename_nodes()
        print("Restructured GTFS graph in {:,.2f} seconds.".format(time.time() - start_time))
        return

    def _check_transfer(self, transfer):
        """Checking for bad nodes in transfers.txt"""
        # start looking at intertransfer edges
        # TODO: may not have to specify just intertransfer edges. could probably do a check of all edges
        interchange = transfer[transfer["from_stop_id"] != transfer["to_stop_id"]]

        # check if all transfer nodes exist in existing nodes map
        station_complex = set().union(interchange["from_stop_id"], interchange["to_stop_id"])

        if station_complex.issubset(self.nodes["parent_station"]):
            # hardcode for mta_new_york_city_transit
            # South Ferry station stop_id since the change in stop_id is not reflected in the 2020-21 gtfs
            if (
                self.nodes["unique_agency_id"].iloc[0] == "mta_new_york_city_transit"
                and transfer[transfer["from_stop_id"] == "142"].empty
            ):
                transfer[["from_stop_id", "to_stop_id"]] = transfer[["from_stop_id", "to_stop_id"]].replace("140", "142")
                return transfer
        else:
            missing_nodes = list(set(station_complex).difference(self.nodes["parent_station"]))
            missing_nodes_transfer = interchange[
                (
                    interchange["from_stop_id"].isin(missing_nodes)
                    | interchange["to_stop_id"].isin(missing_nodes)
                )
            ]
            interchange = transfer.drop(missing_nodes_transfer.index)
            print("Bad nodes dropped from transfer.")
            return transfer

    def _permuted_edges(self, permute, transfer_transport_time):
        """
        Create and return DataFrame containing all relevant attributes of transfer edges.

        Dependent on a gtfs feed with 1 unique agency id, and 1 common network type. Can add conditionals if the
        usage of this function changes.

        TODO:
            - account for non-uniform unique agency id and net_type. maybe set nothing

        Parameters:
            permute -- list
                A list of paired items from itertools.permutations(). Should only contain 2 choices (r = 2)
            transfer_transport_time -- list
                Corresponding list of float that holds the transfer times between paired permute items. Generated
                from transfers.txt or based on a set of rules and assumptions about transferring.
        """
        return pd.concat(
            [pd.DataFrame(
                data={
                    "node_id_from": [permute[i][0]],
                    "node_id_to": [permute[i][1]],
                    "weight": [transfer_transport_time[i]],
                    "unique_agency_id": [self.agency],
                    "route_type": ["transfer"],
                    "net_type": [self.net_type],
                }
            ) for i in range(len(permute))], ignore_index=True)

    def _build_intratransfer_edges(self, transfer, base_transfer_time):
        """Builds transfer edges that exists for nodes from the same parent station."""
        # initialize dataframes
        transfer_nodes = self.nodes.copy()
        transfer_edges = pd.DataFrame()
        error_nodes = pd.DataFrame(columns=transfer_nodes.columns)
        error_nodes.index.name = transfer_nodes.index.name

        # building transfer edges for nodes with same parent_station - refers to transfer_nodes
        start_time = time.time()
        while not transfer_nodes.empty:
            # starts looking at parent_station in transfer to permute edges
            if not pd.isnull(transfer_nodes["parent_station"][0]):
                parent_station = transfer_nodes["parent_station"][0]
                child_station = list(
                    transfer_nodes[
                        transfer_nodes["parent_station"] == parent_station
                    ].index
                )

                if len(child_station) < 2:
                    # if there's only one incoming/outgoing bound service, but not both, then no transfer edge exists
                    pass
                elif len(child_station) == 2:
                    # if there are two child stations only, assume they are incoming/outgoing services
                    permute = list(itertools.permutations(child_station, 2))
                    min_transfer_time = transfer["min_transfer_time"][
                        transfer["from_stop_id"] == parent_station
                    ]
                    transfer_transport_time = [
                        min_transfer_time.iloc[0]
                        if not min_transfer_time.empty
                        else base_transfer_time
                        for i in permute
                    ]
                    transfer_edges = transfer_edges.append(
                        self._permuted_edges(permute, transfer_transport_time),
                        ignore_index=True,
                    )
                else:
                    # if there are more than two child stations, than there are more than one routes going through
                    permute = list(itertools.permutations(child_station, 2))
                    min_transfer_time = transfer["min_transfer_time"][
                        transfer["from_stop_id"] == parent_station
                    ]
                    transfer_transport_time = [
                        min_transfer_time.iloc[0]
                        if (i[0].split("_", 1)[1] != i[1].split("_", 1)[1])
                        and (not min_transfer_time.empty)
                        else base_transfer_time
                        for i in permute
                    ]
                    transfer_edges = transfer_edges.append(
                        self._permuted_edges(permute, transfer_transport_time),
                        ignore_index=True,
                    )
            else:
                error_nodes = error_nodes.append(transfer_nodes.iloc[0])

            # drop all nodes with the same parent_station
            transfer_nodes = transfer_nodes.drop(
                transfer_nodes[transfer_nodes["parent_station"] == parent_station].index
            )

        # TODO: if building transfer edges for other transit feeds
        # TODO: change to logging in implementation
        if len(error_nodes) > 0:
            self.error_nodes = error_nodes
            print(
                "There are empty parent_stations in transit_nodes. Code in jumps to stop_id to continue building "
                "transfer edges. Check error_nodes for list of problematic nodes."
            )

        print(
            "Intra-transfer edges built in {:,.2f} seconds.".format(
                time.time() - start_time
            )
        )
        return transfer_edges

    def _build_intertransfer_edges(self, transfer, base_transfer_time):
        """Builds transfer edges between nodes that have different parent stations."""
        # initialize various dataframes
        transfer_edges = pd.DataFrame()
        interchange = transfer[transfer["from_stop_id"] != transfer["to_stop_id"]]

        # permutate transfer edge (parent station nodes) with the corresponding child ones
        # find all child stations relevant to a given transfer edge
        start_time = time.time()
        for i in range(len(interchange)):
            child_station = list(
                self.nodes[
                    (
                        self.nodes["parent_station"]
                        == interchange["from_stop_id"].iloc[i]
                    )
                    | (
                        self.nodes["parent_station"]
                        == interchange["to_stop_id"].iloc[i]
                    )
                ].index
            )
            permute = list(itertools.permutations(child_station, 2))
            min_transfer_time = interchange["min_transfer_time"].iloc[i]
            transfer_transport_time = [
                min_transfer_time
                if i[0].split("_", 1)[1] != i[1].split("_", 1)[1]
                else base_transfer_time
                for i in permute
            ]
            transfer_edges = transfer_edges.append(
                self._permuted_edges(permute, transfer_transport_time),
                ignore_index=True,
            )

        print(
            "Inter-transfer edges built in {:,.2f} seconds.".format(
                time.time() - start_time
            )
        )
        return transfer_edges

    def _expected_wait_times(self, transfer_edges):
        """
        Create headway averages based on time series graph. Assumes uniform distribution. May also work even
        if time series graph was converted to a time-dependent graph, such as an averaged weight static graph.
        """
        start_time = time.time()
        # could optimize by grouping all the same 'node_id_to' and assigning the headways respectively
        headway_wait_time = (
            array_(
                [
                    self.edges["weight"][
                        self.edges["node_id_to"] == transfer_edges["node_id_to"].iloc[i]
                    ].mean()
                    for i in range(len(transfer_edges))
                ],
                dtype="f",
            )
            / 2
        )
        print(
            "Adding headway wait time took {:,.2f} seconds.".format(
                time.time() - start_time
            )
        )
        return headway_wait_time

    def build_transfer_edges(self):
        """
        Main method to build transfer edges. Imports transfers.txt and processes it to generate all possible
        edges between existing nodes in self.
        """
        start_time = time.time()
        # import transfer txt
        transfer = pd.read_csv(self.feed_path + "/transfers.txt")
        transfer["min_transfer_time"] = transfer["min_transfer_time"] / 60
        base_transfer_time = (
            2  # minimum time in mins to transfer by walking or otherwise
        )

        intra_edges = self._build_intratransfer_edges(transfer, base_transfer_time)
        inter_edges = self._build_intertransfer_edges(transfer, base_transfer_time)

        self.transfer_edges = pd.concat([intra_edges, inter_edges], ignore_index=True)

        self.expected_wait_time = self._expected_wait_times(self, self.transfer_edges)
        self.transfer_edges["weight"] += self.expected_wait_time
        print(
            "Inter-transfer edges built in {:,.2f} seconds.".format(
                time.time() - start_time
            )
        )
        return

    def add_transfer_edges(self):
        """Adds transfer edges to self.edges."""
        if self.transfer_edges.empty:
            print(
                "Transfer edges does not exist in for instance. Instantiating with build_transfer_edges."
            )
            self.build_transfer_edges()
            self.edges = self.edges.append(self.transfer_edges)
        else:
            self.edges = self.edges.append(self.transfer_edges)
        return

    def averaged_edges(self):
        """
        Averages the edges map based on weight.
        This assumes that all other attributes in edges, except weight, are the same.
        """
        cols = [
            "node_id_from",
            "node_id_to",
            "unique_agency_id",
            "route_type",
            "net_type",
        ]
        self.averaged_edges = (
            self.edges.groupby(cols)
            .agg(total_trips=("weight", "count"), weight=("weight", "mean"))
            .reset_index()
            .copy()
        )
        return

    def save_graph(self, path):
        """Saves graph as csv. Can be used for visualization on Gephi."""

        name = [
            "gtfs-nodes",
            self.day,
            self.timerange[0],
            self.timerange[1],
            today,
            ".csv",
        ]
        path = path + "-".join(name[:-1]) + name[-1]
        self.nodes.to_csv(path, index_label="Id")

        # renaming headers to match Gephi formatting. could probably get away with a hard string replace
        header = list(self.edges.columns)
        header = [
            "Source" if i == "node_id_from" else "Target" if i == "node_id_to" else i
            for i in header
        ]
        name = [
            "gtfs-edges",
            self.day,
            self.timerange[0],
            self.timerange[1],
            today,
            ".csv",
        ]
        path = path + "-".join(name[:-1]) + name[-1]
        self.edges.to_csv(path, index=False, header=header)
        return

    @classmethod
    def load_graph(cls, nodes_path, edges_path):
        """
        Loads csv exported GtfsGraph as a GtfsGraph.

        Needs to have attribute recall abilities, such as the class methods called for a given instance.
        """
        return
