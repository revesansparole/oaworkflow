"""This module provide an implementation of a subportgraph"""


from port_graph import InvalidVertex, InvalidEdge, InvalidPort


class SubPortGraph(object):
    """ Provide a view on a sub part of a portgraph.

    Mainly for evaluation purposes.
    No edition allowed.
    """
    def __init__(self, portgraph, vids=()):
        """ Construct a view on portgraph.

        args:
            - portgraph (DataFlow): the portgraph to mirror
            - root_id (vid) default (): view will incorporate
                       only the nodes whose id in this list.
        """
        self._portgraph = portgraph
        self._vids = set(vids)

    def has_vertex(self, vid):
        return vid in self._vids

    def has_edge(self, eid):
        df = self._portgraph
        vids = self._vids
        return df.source(eid) in vids and df.target(eid) in vids

    def has_port(self, pid):
        return self._portgraph.vertex(pid) in self._vids

    def vertices(self):
        for vid in self._portgraph.vertices():
            if self.has_vertex(vid):
                yield vid

    def edges(self):
        for eid in self._portgraph.edges():
            if self.has_edge(eid):
                yield eid

    def in_edges(self, vid):
        if not self.has_vertex(vid):
            raise InvalidVertex("vertex not in view")

        for eid in self._portgraph.in_edges(vid):
            if self.has_edge(eid):
                yield eid

    def out_edges(self, vid):
        if not self.has_vertex(vid):
            raise InvalidVertex("vertex not in view")

        for eid in self._portgraph.out_edges(vid):
            if self.has_edge(eid):
                yield eid

    def nb_in_edges(self, vid):
        return len(tuple(self.in_edges(vid)))

    def nb_out_edges(self, vid):
        return len(tuple(self.out_edges(vid)))

    def in_neighbors(self, vid):
        if not self.has_vertex(vid):
            raise InvalidVertex("vertex not in view")

        for nid in self._portgraph.in_neighbors(vid):
            if self.has_vertex(nid):
                yield nid

    def out_neighbors(self, vid):
        if not self.has_vertex(vid):
            raise InvalidVertex("vertex not in view")

        for nid in self._portgraph.out_neighbors(vid):
            if self.has_vertex(nid):
                yield nid

    def source_port(self, eid):
        if self.has_edge(eid):
            return self._portgraph.source_port(eid)
        else:
            raise InvalidEdge("Edge not in view")

    def target_port(self, eid):
        if self.has_edge(eid):
            return self._portgraph.target_port(eid)
        else:
            raise InvalidEdge("Edge not in view")

    def ports(self, vid=None):
        if vid is not None and not self.has_vertex(vid):
            raise InvalidVertex("vertex not in view")

        for pid in self._portgraph.ports(vid):
            if self.has_port(pid):
                yield pid

    def in_ports(self, vid=None):
        if vid is not None and not self.has_vertex(vid):
            raise InvalidVertex("vertex not in view")

        for pid in self._portgraph.in_ports(vid):
            if self.has_port(pid):
                yield pid

    def out_ports(self, vid=None):
        if vid is not None and not self.has_vertex(vid):
            raise InvalidVertex("vertex not in view")

        for pid in self._portgraph.out_ports(vid):
            if self.has_port(pid):
                yield pid

    def is_in_port(self, pid):
        return self._portgraph.is_in_port(pid)

    def is_out_port(self, pid):
        return self._portgraph.is_out_port(pid)

    def vertex(self, pid):
        return self._portgraph.vertex(pid)

    def connected_edges(self, pid):
        if not self.has_port(pid):
            raise InvalidPort("port not in view")

        for eid in self._portgraph.connected_edges(pid):
            if self.has_edge(eid):
                yield eid

    def connected_ports(self, pid):
        if not self.has_port(pid):
            raise InvalidPort("port not in view")

        for pid in self._portgraph.connected_ports(pid):
            if self.has_port(pid):
                yield pid

    def nb_connections(self, pid):
        if not self.has_port(pid):
            raise InvalidPort("port not in view")

        return len(tuple(self.connected_edges(pid)))

    def local_id(self, pid):
        return self._portgraph.local_id(pid)

    def in_port(self, vid, local_id):
        return self._portgraph.in_port(vid, local_id)

    def out_port(self, vid, local_id):
        return self._portgraph.out_port(vid, local_id)

    def actor(self, vid):
        return self._portgraph.actor(vid)

    def __contains__(self, vid):
        return vid in self._portgraph


def get_upstream_subportgraph(pg, root_pid):
    """ Construct a subportgraph including all the nodes
    upstream a given port.

    args:
        - pg (PortGraph): master portgraph to consider
        - root_pid (pid): id of the port to consider

    return:
        - (SubPortGraph): view on pg that includes only
                          nodes upstream of root_pid
    """
    if not pg.is_in_port(root_pid):
        raise InvalidPort("Port needs to be an input port")

    vids = set()
    front = {pg.vertex(pid) for pid in pg.connected_ports(root_pid)}
    while len(front) > 0:
        vid = front.pop()
        vids.add(vid)
        for nid in pg.in_neighbors(vid):
            if nid not in vids:
                front.add(nid)

    return SubPortGraph(pg, vids)
