""" This module provide an implementation of a way
to store data exchanged between nodes of a portgraph.

THe main goal is to provide a snapshot of what happened
to data in a workflow.
"""

from hashlib import sha512


def hash_port_graph(pg):
    """ Compute hash associated with a portgraph
    to check if editing occurred
    """
    vids = sorted(pg.vertices())
    eids = sorted(pg.edges())
    pids = sorted(pg.ports())
    return sha512(str(vids) + str(eids) + str(pids)).digest()


class WorkflowState(object):
    """ Store outputs of node and provide a way to access them
    """
    def __init__(self, portgraph):
        """ constructor

        args:
            - portgraph (PortGraph)
        """
        self._portgraph = portgraph
        self._init_sha = hash_port_graph(portgraph)

        self._data = {}
        self._param = {}
        self._when = {}

        self._last_evaluation = {}

        self.clear()

    def clear(self):
        """ Clear state
        """
        self._data.clear()
        self._param.clear()
        self._when.clear()
        self._last_evaluation.clear()
        for vid in self._portgraph.vertices():
            self._last_evaluation[vid] = None

    def portgraph(self):
        return self._portgraph

    def portgraph_still_valid(self):
        """ Check portgraph has not been edited since
        the creation of this state.
        """
        sha = hash_port_graph(self._portgraph)
        return sha == self._init_sha

    def items(self):
        """ Iterate on all pid, values stored in this state.

        yield: (pid, value)
        """
        return self._data.items()

    def store(self, pid, data):
        """ Store some data on an output port of the associated
        portgraph.

        args:
         - pid (pid): global id of port
         - data (any): data to store, no copy
        """
        if self._portgraph.is_in_port(pid):
            raise UserWarning("no storage on input ports")

        self._data[pid] = data

    def store_param(self, pid, param, when):
        """ Store some data used as parameters on lonely input ports.

        args:
         - pid (pid): non connected input port
         - param (any): value used as parameter
         - when (exec_id): id of execution when this action occurs
        """
        if self._portgraph.is_out_port(pid):
            raise UserWarning("no params associated to output ports")

        if self._portgraph.nb_connections(pid) > 0:
            raise UserWarning("no params associated to connected ports")

        self._param[pid] = param
        self._when[pid] = when

    def cmp_port_priority(self, pid1, pid2):
        """ Compare port priority.

        Use pids for comparison
        """
        return cmp(pid1, pid2)

    def get(self, pid):
        """ Retrieve data associated to a port.

        args:
         - pid (pid): id of port (in or out)
        """
        pg = self._portgraph

        if pg.is_out_port(pid):
            return self._data[pid]
        else:
            npids = list(pg.connected_ports(pid))
            if len(npids) == 0:
                # lonely input port
                return self._param[pid]
            elif len(npids) == 1:
                return self.get(npids[0])
            else:
                npids.sort(self.cmp_port_priority)
                return [self.get(nid) for nid in npids]

    def when(self, pid):
        """ Retrieve execution id of storage

        For output ports, execution id of associated node
        evaluation.

        For lonely input ports, execution id of when associated
        param was stored.

        For all other input ports, biggest execution id of connected
        output ports.

        args:
         - pid (pid): id of port to check
        """
        pg = self._portgraph

        if pg.is_out_port(pid):
            return self.last_evaluation(pg.vertex(pid))
        else:
            npids = list(pg.connected_ports(pid))
            if len(npids) == 0:
                # lonely input port
                return self._when[pid]
            else:
                return min(self.when(pid) for pid in npids)

    def is_ready_for_evaluation(self):
        """ Test whether the state contains enough information
        to evaluate the associated portgraph.

        Simply check that each lonely input port has
        some data attached to it.
        """
        pg = self._portgraph
        param = self._param

        return all(pid in param for pid in pg.in_ports()
                   if pg.nb_connections(pid) == 0)

    def last_evaluation(self, vid):
        """ Retrieve execution id of last evaluation of this node.

        args:
            - vid (vid): id of actor/task

        return:
            - (eid): execution id of last evaluation or None if
                     the node has not been evaluated yet.
        """
        return self._last_evaluation[vid]

    def set_last_evaluation(self, vid, exec_id):
        """ Store execution id of last evaluation of the node

        args:
            - vid (vid): id of actor/task.
            - exec_id (id): id of last execution that evaluated
                            this node.
        """
        self._last_evaluation[vid] = exec_id
