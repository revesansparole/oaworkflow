""" A Port graph defines a graph whose edges connect
to identified ports on vertices instead of directly to
a given vertex.
"""

from openalea.container.id_generator import IdGenerator
from openalea.container.property_graph import (PropertyGraph,
                                                InvalidVertex,
                                                InvalidEdge)
from openalea.container.graph import GraphError


class InvalidPort(GraphError, KeyError):
    """ Exception raised when a wrong port id is provided
    """
    pass


class Port(object):
    """ Simple structure to maintain some port property.
    A port is an entry point to a vertex
    """

    def __init__(self, vid, local_pid, is_out_port):
        # internal data to access from dataflow
        self.vid = vid
        self.local_pid = local_pid
        self.is_out_port = is_out_port


class PortGraph(PropertyGraph):
    """ A Port graph defines a graph whose edges connect
    to identified ports on vertices instead of directly to
    a given vertex.
    """

    def __init__(self):
        PropertyGraph.__init__(self)
        self._ports = {}
        self._pid_generator = IdGenerator()

        self.add_edge_property("_source_port")
        self.add_edge_property("_target_port")

        self.add_vertex_property("_ports")
        self.add_vertex_property("_actor")

    ####################################################
    #
    #        edge port view
    #
    ####################################################
    def source_port(self, eid):
        """ Out port of the source vertex of the edge.

        args:
            - eid (eid): id of edge

        return:
            - (pid): id of port
        """
        return self.edge_property("_source_port")[eid]

    def target_port(self, eid):
        """ In port of the target vertex of the edge.

        args:
            - eid (eid): id of edge

        return:
            - (pid): id of port
        """
        return self.edge_property("_target_port")[eid]

    ####################################################
    #
    #        vertex port view
    #
    ####################################################
    def ports(self, vid=None):
        """ Iterates on all ports.

        If vid is None, iterates on all ports
        in the dataflow.
        Else, iterates only on the ports of
        the given vertex.

        args:
            - vid (vid): id of vertex

        return:
            - (iter of pid)
        """
        if vid is None:
            return iter(self._ports)
        else:
            try:
                ports = self.vertex_property("_ports")[vid]
                return iter(ports)
            except KeyError:
                raise InvalidVertex("vertex %d does not exist" % vid)

    def in_ports(self, vid=None):
        """ Iterates on all in ports.

        If vid is None, iterates on all in ports
        in the dataflow.
        Else, iterates only on the in ports of
        the given vertex.

        args:
            - vid (vid): id of vertex

        return:
            - (iter of pid)
        """
        for pid in self.ports(vid):
            if self.is_in_port(pid):
                yield pid

    def out_ports(self, vid=None):
        """ Iterates on all out ports.

        If vid is None, iterates on all out ports
        in the dataflow.
        Else, iterates only on the out ports of
        the given vertex.

        args:
            - vid (vid): id of vertex

        return:
            - (iter of pid)
        """
        for pid in self.ports(vid):
            if self.is_out_port(pid):
                yield pid

    ####################################################
    #
    #        port view
    #
    ####################################################
    def is_in_port(self, pid):
        """ Test whether a port is an input for its vertex.

        args:
            - pid (pid): id of port to consider

        return:
            - (bool)
        """
        try:
            return not self._ports[pid].is_out_port
        except KeyError:
            raise InvalidPort("port %s does not exist" % pid)

    def is_out_port(self, pid):
        """ Test whether a port is an output for its vertex.

        args:
            - pid (pid): id of port to consider

        return:
            - (bool)
        """
        try:
            return self._ports[pid].is_out_port
        except KeyError:
            raise InvalidPort("port %s does not exist" % pid)

    def vertex(self, pid):
        """ Find id of the vertex who own the port.

        args:
            - pid (pid): id of port to consider

        return:
            - (vid)
        """
        try:
            return self._ports[pid].vid
        except KeyError:
            raise InvalidPort("port %d does not exist" % pid)

    def connected_edges(self, pid):
        """ Iterate on all edges connected to this port.

        args:
            - pid (pid): id of port to consider

        return:
            - (iter of eid)
        """
        vid = self.vertex(pid)
        if self.is_out_port(pid):
            for eid in self.out_edges(vid):
                if self.source_port(eid) == pid:
                    yield eid
        else:
            for eid in self.in_edges(vid):
                if self.target_port(eid) == pid:
                    yield eid

    def connected_ports(self, pid):
        """ Iterate on all ports connected to this port.

        args:
            - pid (pid): id of port to consider

        return:
            - (iter of pid)
        """
        if self.is_out_port(pid):
            for eid in self.connected_edges(pid):
                yield self.target_port(eid)
        else:
            for eid in self.connected_edges(pid):
                yield self.source_port(eid)

    def nb_connections(self, pid):
        """ Compute number of edges connected to a given port.

        args:
            - pid (pid): id of port

        return:
            - (int)
        """
        return len(tuple(self.connected_edges(pid)))

    ####################################################
    #
    #        local port concept
    #
    ####################################################
    def local_id(self, pid):
        """ Find local id of a port.

        args:
            - pid (pid): id of port

        return:
            - (local pid)
        """
        try:
            return self._ports[pid].local_pid
        except KeyError:
            raise InvalidPort("port %s does not exist" % pid)

    def in_port(self, vid, local_pid):
        """ Find global port id of a given input port.

        args:
            - vid (vid): id of vertex who own the port
            - local_pid (pid): local id of the port

        return:
            - (pid)
        """
        for pid in self.in_ports(vid):
            if self._ports[pid].local_pid == local_pid:
                return pid

        msg = "local pid '%s' does not exist for vertex %d" % (local_pid, vid)
        raise InvalidPort(msg)

    def out_port(self, vid, local_pid):
        """ Find global port id of a given output port.

        args:
            - vid (vid): id of vertex who own the port
            - local_pid (pid): local id of the port

        return:
            - (pid)
        """
        for pid in self.out_ports(vid):
            if self._ports[pid].local_pid == local_pid:
                return pid

        msg = "local pid '%s' does not exist for vertex %d" % (local_pid, vid)
        raise InvalidPort(msg)

    #####################################################
    #
    #        associated actor
    #
    #####################################################
    def actor(self, vid):
        """ Return actor associated to a given vertex.

        return:
            - (IActor)
        """
        try:
            return self.vertex_property("_actor")[vid]
        except KeyError:
            raise InvalidVertex("vertex %s does not exist" % vid)

    def set_actor(self, vid, actor):
        """ Associate an actor to a given vertex.

        args:
            - vid (vid): id of vertex
            - actor (IActor): a function like type of object
        """
        if vid not in self:
            raise InvalidVertex("vertex %d does not exist" % vid)

        if actor is not None:
            # test actor inputs vs vertex in ports
            inputs = set(actor.inputs())
            inports = set(self.local_id(pid) for pid in self.in_ports(vid))
            if inputs != inports:
                msg = "Ports of vertex and inputs of node do not match"
                raise InvalidPort(msg)

            outputs = set(actor.outputs())
            outports = set(self.local_id(pid) for pid in self.out_ports(vid))
            if outputs != outports:
                msg = "Ports of vertex and outputs of node do not match"
                raise InvalidPort(msg)

        self.vertex_property("_actor")[vid] = actor

    # TODO: one day update this function to accept already existing
    # vertices with no actor and create only relevant ports
    def add_actor(self, actor, vid=None):
        """ Create a vertex and the corresponding ports
        and associate it with the given actor.

        args:
            - actor (IActor): a function like type of object
            - vid (vid): id of vertex to use. If None one will
                         be created.

        return:
            - vid (vid): id of the vertex that was created
        """
        vid = self.add_vertex(vid)

        try:
            for key in actor.inputs():
                self.add_in_port(vid, key)

            for key in actor.outputs():
                self.add_out_port(vid, key)

            self.set_actor(vid, actor)
        except AttributeError:
            self.remove_vertex(vid)
            raise

        return vid

    #####################################################
    #
    #        mutable concept
    #
    #####################################################
    def add_in_port(self, vid, local_pid, pid=None):
        """ Add a new input port to a vertex.

        args:
            - vid (vid): id of vertex who will own the port.
            - local_pid (pid): local identifier for the port.
            - pdi (pid): global pid for the port. If None
                         a new one will be created

        return:
            - pid (pid): global id of the created port.
        """
        if vid not in self:
            raise InvalidVertex("vertex %d does not exists" % vid)

        for tpid in self.in_ports(vid):
            if self.local_id(tpid) == local_pid:
                msg = "port %s already exists for this vertex" % local_pid
                raise InvalidPort(msg)

        pid = self._pid_generator.get_id(pid)

        self._ports[pid] = Port(vid, local_pid, False)
        self.vertex_property("_ports")[vid].add(pid)

        return pid

    def add_out_port(self, vid, local_pid, pid=None):
        """ Add a new output port to a vertex.

        args:
            - vid (vid): id of vertex who will own the port.
            - local_pid (pid): local identifier for the port.
            - pdi (pid): global pid for the port. If None
                         a new one will be created

        return:
            - pid (pid): global id of the created port.
        """
        if vid not in self:
            raise InvalidVertex("vertex %d does not exists" % vid)

        for tpid in self.out_ports(vid):
            if self.local_id(tpid) == local_pid:
                msg = "port %s already exists for this vertex" % local_pid
                raise InvalidPort(msg)

        pid = self._pid_generator.get_id(pid)

        self._ports[pid] = Port(vid, local_pid, True)
        self.vertex_property("_ports")[vid].add(pid)

        return pid

    def remove_port(self, pid):
        """ Remove a port and all connections
        attached to this port.

        args:
            - pid (pid): global id of port to remove
        """
        for eid in list(self.connected_edges(pid)):
            self.remove_edge(eid)

        self.vertex_property("_ports")[self.vertex(pid)].remove(pid)
        self._pid_generator.release_id(pid)

        del self._ports[pid]

    def add_edge(self, edge=None, eid=None):
        """ Usage of this method is forbidden
        """
        raise UserWarning("Call connect instead")

    # TODO: add tests to prevent connections on same vertex?
    # TODO: add tests to prevent duplicating connection?
    def connect(self, source_pid, target_pid, eid=None):
        """ Connect two ports together.

        Connection can only be created between and output port
        and an input port.

        args:
            - source_pid (pid): global id of output port.
            - target_pid (pid): global if of input port.
            - eid (eid): edge id to use. If None, a new one
                        will be assigned.

        return:
            - eid (eid): id of edge used to make the connection.
        """
        if not self.is_out_port(source_pid):
            msg = "source_pid %s is not an output port" % str(source_pid)
            raise InvalidPort(msg)

        if not self.is_in_port(target_pid):
            msg = "target_pid %s is not an input port" % str(target_pid)
            raise InvalidPort(msg)

        eid = PropertyGraph.add_edge(self,
                                     self.vertex(source_pid),
                                     self.vertex(target_pid),
                                     eid)
        self.edge_property("_source_port")[eid] = source_pid
        self.edge_property("_target_port")[eid] = target_pid

        return eid

    def add_vertex(self, vid=None):
        vid = PropertyGraph.add_vertex(self, vid)
        self.vertex_property("_ports")[vid] = set()
        self.set_actor(vid, None)
        return vid

    add_vertex.__doc__ = PropertyGraph.add_vertex.__doc__

    def remove_vertex(self, vid):
        for pid in list(self.ports(vid)):
            self.remove_port(pid)

        PropertyGraph.remove_vertex(self, vid)

    remove_vertex.__doc__ = PropertyGraph.remove_vertex.__doc__

    def clear(self):
        self._ports.clear()
        self._pid_generator = IdGenerator()
        PropertyGraph.clear(self)

    clear.__doc__ = PropertyGraph.clear.__doc__
