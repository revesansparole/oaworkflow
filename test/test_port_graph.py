from nose.tools import assert_raises

from openalea.workflow.node import Node
from openalea.workflow.port_graph import (PortGraph,
                                          InvalidEdge,
                                          InvalidPort,
                                          InvalidVertex)


def test_portgraph_init():
    pg = PortGraph()

    assert pg.nb_vertices() == 0
    assert pg.nb_edges() == 0


def test_portgraph_edge_connected_to_ports():
    pg = PortGraph()
    vid1 = pg.add_vertex()
    vid2 = pg.add_vertex()

    # method add_edge is forbidden
    assert_raises(UserWarning, lambda: pg.add_edge((vid1, vid2)))

    # connect to None port raises InvalidPort
    assert_raises(InvalidPort, lambda: pg.connect(None, None))
    pid1 = pg.add_out_port(vid1, "out")
    pid2 = pg.add_in_port(vid2, "in")
    assert_raises(InvalidPort, lambda: pg.connect(pid1, None))
    assert_raises(InvalidPort, lambda: pg.connect(None, pid2))

    # connect from in port to out port raises InvalidPort
    assert_raises(InvalidPort, lambda: pg.connect(pid2, pid1))

    # test connect is working
    eid = pg.connect(pid1, pid2)
    assert pg.source_port(eid) == pid1
    assert pg.target_port(eid) == pid2


def test_portgraph_ports():
    pg = PortGraph()

    assert_raises(InvalidVertex, lambda: pg.ports(0))

    vid1 = pg.add_vertex()
    vid2 = pg.add_vertex()

    # test new vertices are created without ports
    assert len(tuple(pg.ports(vid1))) == 0
    assert len(tuple(pg.ports())) == 0

    ipid1 = pg.add_in_port(vid1, "in")
    opids = [pg.add_out_port(vid1, "out%d" % i) for i in range(5)]
    ipid2 = pg.add_in_port(vid2, "in")
    opid = pg.add_out_port(vid2, "out")

    assert sorted(pg.ports(vid1)) == sorted([ipid1] + opids)
    assert sorted(pg.ports()) == sorted([ipid1] + opids + [ipid2, opid])


def test_portgraph_in_ports():
    pg = PortGraph()

    assert_raises(InvalidVertex, lambda: tuple(pg.in_ports(0)))

    vid1 = pg.add_vertex()
    vid2 = pg.add_vertex()

    # test new vertices are created without ports
    assert len(tuple(pg.in_ports(vid1))) == 0
    assert len(tuple(pg.in_ports())) == 0

    pids = [pg.add_in_port(vid1, "in%d" % i) for i in range(5)]
    pg.add_out_port(vid1, "out")
    pid = pg.add_in_port(vid2, "in")
    pg.add_out_port(vid2, "out")

    assert sorted(pg.in_ports(vid1)) == sorted(pids)
    assert sorted(pg.in_ports()) == sorted(pids + [pid])


def test_portgraph_out_ports():
    pg = PortGraph()

    assert_raises(InvalidVertex, lambda: tuple(pg.out_ports(0)))

    vid1 = pg.add_vertex()
    vid2 = pg.add_vertex()

    # test new vertices are created without ports
    assert len(tuple(pg.out_ports(vid1))) == 0
    assert len(tuple(pg.out_ports())) == 0

    pg.add_in_port(vid1, "in")
    pids = [pg.add_out_port(vid1, "out%d" % i) for i in range(5)]
    pg.add_in_port(vid2, "in")
    pid = pg.add_out_port(vid2, "out")

    assert sorted(pg.out_ports(vid1)) == sorted(pids)
    assert sorted(pg.out_ports()) == sorted(pids + [pid])


def test_portgraph_is_in_port():
    pg = PortGraph()
    vid = pg.add_vertex()

    assert_raises(InvalidPort, lambda: pg.is_in_port(None))
    assert_raises(InvalidPort, lambda: pg.is_in_port(0))

    pid = pg.add_in_port(vid, "in")
    assert_raises(InvalidPort, lambda: pg.is_in_port(pid + 1))
    assert pg.is_in_port(pid)

    pid = pg.add_out_port(vid, "out")
    assert not pg.is_in_port(pid)


def test_portgraph_is_out_port():
    pg = PortGraph()
    vid = pg.add_vertex()

    assert_raises(InvalidPort, lambda: pg.is_out_port(None))
    assert_raises(InvalidPort, lambda: pg.is_out_port(0))

    pid = pg.add_out_port(vid, "out")
    assert_raises(InvalidPort, lambda: pg.is_out_port(pid + 1))
    assert pg.is_out_port(pid)

    pid = pg.add_in_port(vid, "in")
    assert not pg.is_out_port(pid)


def test_portgraph_vertex():
    pg = PortGraph()

    assert_raises(InvalidPort, lambda: pg.vertex(0))

    vid = pg.add_vertex()
    pid1 = pg.add_in_port(vid, 0)
    pid2 = pg.add_out_port(vid, 0)
    assert pg.vertex(pid1) == vid
    assert pg.vertex(pid2) == vid


def test_portgraph_connected_edges():
    pg = PortGraph()
    vid1 = pg.add_vertex()
    vid2 = pg.add_vertex()
    vid3 = pg.add_vertex()

    assert_raises(InvalidPort, lambda: tuple(pg.connected_edges(0)))

    pid1 = pg.add_in_port(vid1, 0)
    pid2 = pg.add_in_port(vid1, 1)
    pid3 = pg.add_out_port(vid2, 0)
    pid4 = pg.add_out_port(vid3, 0)

    # test vertices and ports are created without connections
    for pid in (pid1, pid2, pid3, pid4):
        assert len(tuple(pg.connected_edges(pid))) == 0

    eid1 = pg.connect(pid3, pid1)
    assert tuple(pg.connected_edges(pid3)) == (eid1,)
    assert tuple(pg.connected_edges(pid1)) == (eid1,)
    for pid in (pid2, pid4):
        assert len(tuple(pg.connected_edges(pid))) == 0

    eid2 = pg.connect(pid4, pid1)
    assert tuple(pg.connected_edges(pid3)) == (eid1,)
    assert tuple(pg.connected_edges(pid4)) == (eid2,)
    assert sorted(pg.connected_edges(pid1)) == sorted((eid1, eid2))
    assert len(tuple(pg.connected_edges(pid2))) == 0

    eid3 = pg.connect(pid4, pid2)
    assert tuple(pg.connected_edges(pid3)) == (eid1,)
    assert sorted(pg.connected_edges(pid4)) == sorted((eid2, eid3))
    assert sorted(pg.connected_edges(pid1)) == sorted((eid1, eid2))
    assert tuple(pg.connected_edges(pid2)) == (eid3,)


def test_portgraph_connected_ports():
    pg = PortGraph()
    vid1 = pg.add_vertex()
    vid2 = pg.add_vertex()
    vid3 = pg.add_vertex()

    assert_raises(InvalidPort, lambda: tuple(pg.connected_ports(0)))

    pid1 = pg.add_in_port(vid1, 0)
    pid2 = pg.add_in_port(vid1, 1)
    pid3 = pg.add_out_port(vid2, 0)
    pid4 = pg.add_out_port(vid3, 0)

    for pid in (pid1, pid2, pid3, pid4):
        assert len(tuple(pg.connected_ports(pid))) == 0

    pg.connect(pid3, pid1)
    assert tuple(pg.connected_ports(pid3)) == (pid1,)
    assert tuple(pg.connected_ports(pid1)) == (pid3,)
    for pid in (pid2, pid4):
        assert len(tuple(pg.connected_ports(pid))) == 0

    pg.connect(pid4, pid1)
    assert tuple(pg.connected_ports(pid3)) == (pid1,)
    assert tuple(pg.connected_ports(pid4)) == (pid1,)
    assert sorted(pg.connected_ports(pid1)) == sorted((pid3, pid4))
    assert len(tuple(pg.connected_ports(pid2))) == 0

    pg.connect(pid4, pid2)
    assert tuple(pg.connected_ports(pid3)) == (pid1,)
    assert sorted(pg.connected_ports(pid4)) == sorted((pid1, pid2))
    assert sorted(pg.connected_ports(pid1)) == sorted((pid3, pid4))
    assert tuple(pg.connected_ports(pid2)) == (pid4,)


def portgraph_nb_connections():
    pg = PortGraph()
    vid1 = pg.add_vertex()
    vid2 = pg.add_vertex()
    vid3 = pg.add_vertex()

    assert_raises(InvalidPort, lambda: pg.nb_connections(0))

    pid1 = pg.add_in_port(vid1, 0)
    pid2 = pg.add_in_port(vid1, 1)
    pid3 = pg.add_out_port(vid2, 0)
    pid4 = pg.add_out_port(vid3, 0)

    for pid in (pid1, pid2, pid3, pid4):
        assert pg.nb_connections(pid) == 0

    pg.connect(pid3, pid1)
    assert pg.nb_connections(pid3) == 1
    assert pg.nb_connections(pid1) == 1
    assert pg.nb_connections(pid2) == 0
    assert pg.nb_connections(pid4) == 0

    pg.connect(pid4, pid1)
    assert pg.nb_connections(pid3) == 1
    assert pg.nb_connections(pid4) == 1
    assert pg.nb_connections(pid1) == 2
    assert pg.nb_connections(pid2) == 0

    pg.connect(pid4, pid2)
    assert pg.nb_connections(pid3) == 1
    assert pg.nb_connections(pid4) == 2
    assert pg.nb_connections(pid1) == 2
    assert pg.nb_connections(pid2) == 1


def test_portgraph_local_id():
    pg = PortGraph()
    vid = pg.add_vertex()

    assert_raises(InvalidPort, lambda: pg.local_id(0))

    for lpid in (0, 1, "a", None):
        pid = pg.add_in_port(vid, lpid)
        assert pg.local_id(pid) == lpid
        pid = pg.add_out_port(vid, lpid)
        assert pg.local_id(pid) == lpid


def test_portgraph_in_port():
    pg = PortGraph()
    assert_raises(InvalidVertex, lambda: pg.in_port(0, None))

    vid = pg.add_vertex()
    assert_raises(InvalidPort, lambda: pg.in_port(vid, None))

    pg.add_out_port(vid, "toto")
    assert_raises(InvalidPort, lambda: pg.in_port(vid, None))

    for lpid in [0, 1, "a", None]:
        assert_raises(InvalidPort, lambda: pg.in_port(vid, "toto"))
        pid = pg.add_in_port(vid, lpid)
        assert pg.in_port(vid, lpid) == pid


def test_portgraph_out_port():
    pg = PortGraph()
    assert_raises(InvalidVertex, lambda: pg.out_port(0, None))

    vid = pg.add_vertex()
    assert_raises(InvalidPort, lambda: pg.out_port(vid, None))

    pg.add_in_port(vid, "toto")
    assert_raises(InvalidPort, lambda: pg.out_port(vid, None))

    for lpid in [0, 1, "a", None]:
        assert_raises(InvalidPort, lambda: pg.out_port(vid, "toto"))
        pid = pg.add_out_port(vid, lpid)
        assert pg.out_port(vid, lpid) == pid


def test_portgraph_actor():
    pg = PortGraph()
    assert_raises(InvalidVertex, lambda: pg.actor(0))

    vid = pg.add_vertex()
    assert_raises(InvalidVertex, lambda: pg.actor(vid + 1))
    assert pg.actor(vid) is None

    actor = Node()
    for key in ("toto", 1, "titi"):
        actor.add_input(key, "descr")
        actor.add_output(key, "descr")

    assert_raises(InvalidPort, lambda: pg.set_actor(vid, actor))

    for key in actor.inputs():
        pg.add_in_port(vid, key)

    for key in actor.outputs():
        pg.add_out_port(vid, key)

    pg.set_actor(vid, actor)
    assert pg.actor(vid) == actor


def test_portgraph_set_actor():
    pg = PortGraph()
    assert_raises(InvalidVertex, lambda: pg.set_actor(0, None))

    actor = Node()
    for key in ("toto", 1, "titi"):
        actor.add_input(key, "descr")
        actor.add_output(key, "descr")

    assert_raises(InvalidVertex, lambda: pg.set_actor(0, actor))

    vid = pg.add_vertex()
    pg.set_actor(vid, None)
    assert pg.actor(vid) is None
    assert_raises(InvalidPort, lambda: pg.set_actor(vid, actor))

    for key in actor.inputs():
        pg.add_in_port(vid, key)

    assert_raises(InvalidPort, lambda: pg.set_actor(vid, actor))

    for key in actor.outputs():
        pg.add_out_port(vid, key)

    pg.set_actor(vid, actor)
    assert pg.actor(vid) == actor

    pg.set_actor(vid, None)
    assert pg.actor(vid) is None


def test_portgraph_set_actor_port_order_not_important():
    pg = PortGraph()

    actor = Node()
    keys = ("toto", 1, "titi")
    for key in keys:
        actor.add_input(key, "descr")
        actor.add_output(key, "descr")

    vid = pg.add_vertex()
    for k in reversed(keys):
        pg.add_in_port(vid, k)
        pg.add_out_port(vid, k)

    pg.set_actor(vid, actor)
    assert pg.actor(vid) == actor


def test_portgraph_add_actor():
    pg = PortGraph()
    vid1 = pg.add_vertex()
    actor = Node()
    keys = {"toto", 1, "titi"}
    for key in keys:
        actor.add_input(key, "descr")
        actor.add_output(key, "descr")

    # bad actor
    assert_raises(AttributeError, lambda: pg.add_actor(None))
    assert len(pg) == 1
    assert_raises(AttributeError, lambda: pg.add_actor(None, vid1 + 1))
    assert len(pg) == 1
    # vertex id already used
    assert_raises(InvalidVertex, lambda: pg.add_actor(actor, vid1))
    assert len(pg) == 1

    for key in actor.inputs():
        pg.add_in_port(vid1, key)

    for key in actor.outputs():
        pg.add_out_port(vid1, key)

    pg.set_actor(vid1, actor)
    # vertex id already used
    assert_raises(InvalidVertex, lambda: pg.add_actor(actor, vid1))
    assert len(pg) == 1

    vid2 = pg.add_actor(actor)
    assert pg.actor(vid2) == actor
    assert set(actor.inputs()) == keys
    assert set(pg.local_id(pid) for pid in pg.in_ports(vid2)) == keys
    assert set(actor.outputs()) == keys
    assert set(pg.local_id(pid) for pid in pg.out_ports(vid2)) == keys


def test_portgraph_add_in_port():
    pg = PortGraph()
    assert_raises(InvalidVertex, lambda: pg.add_in_port(0, "toto"))

    vid = pg.add_vertex()
    pid = pg.add_in_port(vid, "port")

    # raise error if reuse same global port id
    assert_raises(IndexError, lambda: pg.add_in_port(vid, "toto", pid))
    # raise error if reuse same local port id
    assert_raises(InvalidPort, lambda: pg.add_in_port(vid, "port"))

    assert tuple(pg.in_ports(vid)) == (pid,)
    assert pg.local_id(pid) == "port"
    assert pg.in_port(vid, "port") == pid


def test_portgraph_add_out_port():
    pg = PortGraph()
    assert_raises(InvalidVertex, lambda: pg.add_out_port(0, "toto"))

    vid = pg.add_vertex()
    pid = pg.add_out_port(vid, "port")

    # raise error if reuse same global port id
    assert_raises(IndexError, lambda: pg.add_out_port(vid, "toto", pid))
    # raise error if reuse same local port id
    assert_raises(InvalidPort, lambda: pg.add_out_port(vid, "port"))

    assert tuple(pg.out_ports(vid)) == (pid,)
    assert pg.local_id(pid) == "port"
    assert pg.out_port(vid, "port") == pid


def test_portgraph_remove_port():
    pg = PortGraph()
    vid1 = pg.add_vertex()
    vid2 = pg.add_vertex()

    pid1 = pg.add_in_port(vid1, "in")
    pid2 = pg.add_out_port(vid2, "out")

    pg.connect(pid2, pid1)

    # raise error if global port id dos not exist
    assert_raises(InvalidPort, lambda: pg.remove_port(pid1 + pid2 + 1))

    pg.remove_port(pid1)
    assert len(tuple(pg.ports(vid1))) == 0
    assert tuple(pg.ports(vid2)) == (pid2,)
    assert pg.nb_connections(pid2) == 0


def test_portgraph_connect():
    pg = PortGraph()
    vid1 = pg.add_vertex()
    vid2 = pg.add_vertex()

    # invalid ports
    assert_raises(InvalidPort, lambda: pg.connect(0, 1))
    # invalid source port
    pid1 = pg.add_in_port(vid1, "in")
    assert_raises(InvalidPort, lambda: pg.connect(pid1 + 1, pid1))
    # invalid out port
    pid2 = pg.add_out_port(vid2, "out")
    assert_raises(InvalidPort, lambda: pg.connect(pid2, pid1 + pid2 + 1))
    # edge connection from in to out raise error
    assert_raises(InvalidPort, lambda: pg.connect(pid1, pid2))

    eid = pg.connect(pid2, pid1)
    assert pg.source_port(eid) == pid2
    assert pg.target_port(eid) == pid1
    assert tuple(pg.connected_edges(pid1)) == (eid,)
    assert tuple(pg.connected_edges(pid2)) == (eid,)
    assert pid1 in pg.connected_ports(pid2)
    assert pid2 in pg.connected_ports(pid1)

    # can not duplicate edge
    assert_raises(InvalidEdge, lambda: pg.connect(pid2, pid1, eid))


def test_portgraph_add_vertex():
    pg = PortGraph()

    vid = pg.add_vertex()
    assert pg.nb_vertices() == 1
    assert_raises(InvalidVertex, lambda: pg.add_vertex(vid))

    assert len(tuple(pg.ports(vid))) == 0
    assert pg.actor(vid) is None


def test_portgraph_remove_vertex():
    pg = PortGraph()
    vid1 = pg.add_vertex()
    vid2 = pg.add_vertex()

    pid1 = pg.add_in_port(vid1, "in")
    pid2 = pg.add_out_port(vid2, "out")

    pg.connect(pid2, pid1)

    assert_raises(InvalidVertex, lambda: pg.remove_vertex(vid1 + vid2 + 1))

    pg.remove_vertex(vid1)
    assert tuple(pg.ports()) == (pid2,)
    assert_raises(InvalidVertex, lambda: pg.ports(vid1).next())
    assert tuple(pg.ports(vid2)) == (pid2,)
    assert pg.nb_connections(pid2) == 0
    assert pg.nb_neighbors(vid2) == 0


def test_portgraph_clear():
    pg = PortGraph()
    vid1 = pg.add_vertex()
    vid2 = pg.add_vertex()

    pid1 = pg.add_in_port(vid1, "in")
    pid2 = pg.add_out_port(vid2, "out")

    pg.connect(pid2, pid1)

    pg.clear()

    assert pg.nb_vertices() == 0
    assert pg.nb_edges() == 0
    assert len(tuple(pg.ports())) == 0


def test_portgraph_big():
    pg = PortGraph()
    vid1 = pg.add_vertex()
    pid11 = pg.add_out_port(vid1, "out")
    vid2 = pg.add_vertex()
    pid21 = pg.add_out_port(vid2, "out")

    vid3 = pg.add_vertex()
    pid31 = pg.add_in_port(vid3, "in1")
    pid32 = pg.add_in_port(vid3, "in2")
    pid33 = pg.add_out_port(vid3, "res")

    vid4 = pg.add_vertex()
    pid41 = pg.add_in_port(vid4, "in")

    eid1 = pg.connect(pid11, pid31)
    eid2 = pg.connect(pid21, pid32)
    pg.connect(pid33, pid41)

    assert pg.source_port(eid1) == pid11
    assert pg.target_port(eid2) == pid32
    assert set(pg.out_ports(vid1)) == {pid11}
    assert set(pg.in_ports(vid3)) == {pid31, pid32}
    assert set(pg.ports(vid3)) == {pid31, pid32, pid33}
    assert pg.is_in_port(pid31)
    assert pg.is_out_port(pid11)
    assert pg.vertex(pid11) == vid1
    assert set(pg.connected_ports(pid11)) == {pid31}
    assert set(pg.connected_edges(pid21)) == {eid2}
    assert pg.out_port(vid1, "out") == pid11
    assert pg.in_port(vid3, "in1") == pid31

    assert_raises(InvalidPort, lambda: pg.connect(pid11, pid33))

    pg.remove_port(pid33)
    assert set(pg.connected_ports(pid41)) == set()
    assert set(pg.out_edges(vid3)) == set()
    assert_raises(InvalidPort, lambda: pg.is_in_port(pid33))
