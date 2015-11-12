from nose.tools import assert_raises

from workflow.port_graph import (PortGraph,
                                 InvalidEdge,
                                 InvalidVertex,
                                 InvalidPort)
from workflow.sub_port_graph import (SubPortGraph,
                                     get_upstream_subportgraph)


def get_pg():
    pg = PortGraph()
    pg.add_vertex(0)
    pg.add_out_port(0, "out", 0)
    pg.add_vertex(1)
    pg.add_out_port(1, "out", 1)

    pg.add_vertex(2)
    pg.add_in_port(2, "in1", 2)
    pg.add_in_port(2, "in2", 3)
    pg.add_out_port(2, "res", 4)

    pg.add_vertex(3)
    pg.add_in_port(3, "in", 5)

    pg.add_vertex(4)
    pg.add_out_port(4, "out", 6)

    pg.connect(0, 2, 0)
    pg.connect(1, 3, 1)
    pg.connect(4, 5, 2)
    pg.connect(6, 3, 3)

    return pg


def test_subportgraph_init():
    pg = PortGraph()
    sub = SubPortGraph(pg)

    assert len(tuple(sub.vertices())) == 0


def test_subportgraph_vertices():
    pg = get_pg()

    sub = SubPortGraph(pg, (0, 2))
    assert set(sub.vertices()) == {0, 2}


def test_subportgraph_edges():
    pg = get_pg()

    sub = SubPortGraph(pg, (0, 2))
    assert set(sub.edges()) == {0}


def test_subportgraph_in_edges():
    pg = get_pg()

    sub = SubPortGraph(pg, (0, 2))
    assert_raises(InvalidVertex, lambda: tuple(sub.in_edges(10)))
    assert set(sub.in_edges(2)) == {0}
    assert sub.nb_in_edges(2) == 1


def test_subportgraph_out_edges():
    pg = get_pg()

    sub = SubPortGraph(pg, (0, 2))
    assert_raises(InvalidVertex, lambda: tuple(sub.out_edges(10)))
    assert set(sub.out_edges(0)) == {0}
    assert sub.nb_out_edges(0) == 1


def test_subportgraph_in_neighbors():
    pg = get_pg()

    sub = SubPortGraph(pg, (0, 2))
    assert_raises(InvalidVertex, lambda: tuple(sub.in_neighbors(10)))
    assert set(sub.in_neighbors(2)) == {0}


def test_subportgraph_out_neighbors():
    pg = get_pg()

    sub = SubPortGraph(pg, (0, 2))
    assert_raises(InvalidVertex, lambda: tuple(sub.out_neighbors(10)))
    assert set(sub.out_neighbors(0)) == {2}


def test_subportgraph_port():
    pg = get_pg()

    sub = SubPortGraph(pg, (0, 2))

    assert sub.source_port(0) == pg.source_port(0)
    assert_raises(InvalidEdge, lambda: sub.source_port(1))
    assert_raises(InvalidEdge, lambda: sub.source_port(2))

    assert sub.target_port(0) == pg.target_port(0)
    assert_raises(InvalidEdge, lambda: sub.target_port(1))
    assert_raises(InvalidEdge, lambda: sub.target_port(2))


def test_subportgraph_ports():
    pg = get_pg()

    sub = SubPortGraph(pg, (0, 2))

    assert set(sub.ports()) == {0, 2, 3, 4}
    assert set(sub.ports(0)) == {0}
    assert_raises(InvalidVertex, lambda: sub.ports(1).next())
    assert set(sub.ports(2)) == {2, 3, 4}
    assert_raises(InvalidVertex, lambda: sub.ports(3).next())


def test_subportgraph_in_ports():
    pg = get_pg()

    sub = SubPortGraph(pg, (0, 2))

    assert set(sub.in_ports()) == {2, 3}
    assert len(set(sub.in_ports(0))) == 0
    assert_raises(InvalidVertex, lambda: sub.in_ports(1).next())
    assert set(sub.in_ports(2)) == {2, 3}
    assert_raises(InvalidVertex, lambda: sub.in_ports(3).next())


def test_subportgraph_out_ports():
    pg = get_pg()

    sub = SubPortGraph(pg, (0, 2))

    assert set(sub.out_ports()) == {0, 4}
    assert set(sub.out_ports(0)) == {0}
    assert_raises(InvalidVertex, lambda: sub.out_ports(1).next())
    assert set(sub.out_ports(2)) == {4}
    assert_raises(InvalidVertex, lambda: sub.out_ports(3).next())


def test_subportgraph_connected_edges():
    pg = get_pg()

    sub = SubPortGraph(pg, (0, 2))

    assert set(sub.connected_edges(0)) == {0}
    assert set(sub.connected_edges(2)) == {0}
    assert len(set(sub.connected_edges(3))) == 0
    assert len(set(sub.connected_edges(4))) == 0
    assert_raises(InvalidPort, lambda: sub.connected_edges(1).next())
    assert_raises(InvalidPort, lambda: sub.connected_edges(5).next())


def test_subportgraph_connected_ports():
    pg = get_pg()

    sub = SubPortGraph(pg, (0, 2))

    assert set(sub.connected_ports(0)) == {2}
    assert set(sub.connected_ports(2)) == {0}
    assert len(set(sub.connected_ports(3))) == 0
    assert len(set(sub.connected_ports(4))) == 0
    assert_raises(InvalidPort, lambda: sub.connected_ports(1).next())
    assert_raises(InvalidPort, lambda: sub.connected_ports(5).next())


def test_subportgraph_nb_connections():
    pg = get_pg()

    sub = SubPortGraph(pg, (0, 2))
    assert sub.nb_connections(0) == 1
    assert sub.nb_connections(2) == 1
    assert sub.nb_connections(3) == 0
    assert sub.nb_connections(4) == 0
    for pid in (1, 5):
        assert_raises(InvalidPort, lambda: sub.nb_connections(pid))


def test_subportgraph_mirror_functions():
    pg = get_pg()

    sub = SubPortGraph(pg, (0, 2))
    assert sub.is_in_port(2)
    assert sub.is_out_port(0)
    assert sub.vertex(0) == 0
    assert sub.local_id(0) == pg.local_id(0)
    assert sub.in_port(2, "in1") == pg.in_port(2, "in1")
    assert sub.out_port(0, "out") == pg.out_port(0, "out")
    assert sub.actor(0) == pg.actor(0)
    assert 0 in sub


def test_subportgraph_get_upstream_subportgraph():
    pg = get_pg()
    assert_raises(InvalidPort, lambda: get_upstream_subportgraph(pg, 0))

    sub = get_upstream_subportgraph(pg, 2)
    assert set(sub.vertices()) == {0}

    sub = get_upstream_subportgraph(pg, 3)
    assert set(sub.vertices()) == {1, 4}

    sub = get_upstream_subportgraph(pg, 5)
    assert set(sub.vertices()) == {0, 1, 2, 4}
