from nose.tools import assert_raises

from openalea.workflow.port_graph import PortGraph
from openalea.workflow.state import WorkflowState
from openalea.workflow.sub_port_graph import get_upstream_subportgraph


def test_ws_is_created_empty():
    pg = PortGraph()
    ws = WorkflowState(pg)
    assert len(tuple(ws.items())) == 0
    assert id(ws.portgraph()) == id(pg)

    pg.add_vertex(0)
    pg.add_in_port(0, "in", 0)
    pg.add_out_port(0, "out", 1)

    ws = WorkflowState(pg)
    assert len(tuple(ws.items())) == 0


def test_ws_dos_not_allow_graph_editing():
    pg = PortGraph()
    ws = WorkflowState(pg)

    assert ws.portgraph_still_valid()
    pg.add_vertex(0)
    assert not ws.portgraph_still_valid()

    ws = WorkflowState(pg)
    assert ws.portgraph_still_valid()
    pg.add_out_port(0, "out", 0)
    assert not ws.portgraph_still_valid()

    pg.add_vertex(1)
    pg.add_in_port(1, "in", 1)
    ws = WorkflowState(pg)
    assert ws.portgraph_still_valid()
    pg.connect(0, 1)
    assert not ws.portgraph_still_valid()

    ws = WorkflowState(pg)
    assert ws.portgraph_still_valid()
    pg.remove_port(0)
    assert not ws.portgraph_still_valid()


def test_ws_can_not_store_data_on_input_port():
    pg = PortGraph()
    pg.add_vertex(0)
    pg.add_in_port(0, "in", 0)
    ws = WorkflowState(pg)

    assert_raises(UserWarning, lambda: ws.store(0, "data"))


def test_ws_retrieve_stored_data_on_output_port():
    pg = PortGraph()
    pg.add_vertex(0)
    pg.add_out_port(0, "out", 0)
    ws = WorkflowState(pg)

    ws.store(0, "data")
    assert_raises(KeyError, lambda: ws.get(1))

    assert ws.get(0) == "data"


def test_ws_store_param_only_on_lonely_input_ports():
    pg = PortGraph()
    pg.add_vertex(0)
    pg.add_in_port(0, "in", 0)
    pg.add_out_port(0, "out", 1)
    pg.add_vertex(1)
    pg.add_in_port(1, "in", 2)
    pg.connect(1, 2)

    ws = WorkflowState(pg)

    assert_raises(UserWarning, lambda: ws.store_param(1, "param", 0))
    assert_raises(UserWarning, lambda: ws.store_param(2, "param", 0))


def test_ws_retrieve_data_on_input_ports():
    pg = PortGraph()
    pg.add_vertex(0)
    pg.add_out_port(0, "out", 0)
    pg.add_vertex(1)
    pg.add_in_port(1, "in", 1)

    # non existing port
    ws = WorkflowState(pg)
    assert_raises(KeyError, lambda: ws.get(10))

    # lonely input port without data
    ws = WorkflowState(pg)
    assert_raises(KeyError, lambda: ws.get(1))

    # lonely input port with data
    ws.store_param(1, "param", 0)
    assert ws.get(1) == "param"

    # connected input port
    pg.connect(0, 1)
    ws = WorkflowState(pg)

    ws.store(0, "data")
    assert ws.get(1) == "data"


def test_ws_retrieve_data_on_multiple_connections_input_ports():
    pg = PortGraph()
    pg.add_vertex(0)
    pg.add_out_port(0, "out", 0)
    pg.add_vertex(1)
    pg.add_out_port(1, "out", 1)
    pg.add_vertex(2)
    pg.add_in_port(2, "in", 2)
    pg.connect(0, 2)
    pg.connect(1, 2)

    ws = WorkflowState(pg)
    assert_raises(KeyError, lambda: ws.get(2))
    ws.store(0, "data0")
    assert_raises(KeyError, lambda: ws.get(2))
    ws.store(1, "data1")
    assert tuple(ws.get(2)) == ("data0", "data1")


def test_ws_is_ready_for_evaluation():
    pg = PortGraph()
    pg.add_vertex(0)
    pg.add_in_port(0, "in", 0)
    pg.add_out_port(0, "out", 1)
    pg.add_vertex(1)
    pg.add_out_port(1, "out", 2)
    pg.add_vertex(2)
    pg.add_in_port(2, "in", 3)
    pg.add_out_port(2, "out", 4)
    pg.connect(1, 3)
    pg.connect(2, 3)

    ws = WorkflowState(pg)
    assert not ws.is_ready_for_evaluation()

    ws.store(1, "data")
    assert not ws.is_ready_for_evaluation()

    ws.store_param(0, "param", 0)
    assert ws.is_ready_for_evaluation()


def test_ws_nodes_not_evaluated_on_creation():
    pg = PortGraph()
    pg.add_vertex(0)
    pg.add_vertex(1)

    ws = WorkflowState(pg)
    assert ws.last_evaluation(0) is None
    assert ws.last_evaluation(1) is None


def test_ws_nodes_last_evaluation():
    pg = PortGraph()
    pg.add_vertex(0)
    pg.add_vertex(1)

    ws = WorkflowState(pg)
    ws.set_last_evaluation(0, 1)
    assert ws.last_evaluation(0) == 1
    assert ws.last_evaluation(1) is None


def test_ws_when_is_none_on_creation():
    pg = PortGraph()
    pg.add_vertex(0)
    pg.add_in_port(0, "in", 0)
    pg.add_out_port(0, "out", 1)

    ws = WorkflowState(pg)
    assert_raises(KeyError, lambda: ws.when(10))


def test_ws_when_is_none_for_non_evaluated_output_port():
    pg = PortGraph()
    pg.add_vertex(0)
    pg.add_out_port(0, "out", 0)

    ws = WorkflowState(pg)
    assert ws.when(0) is None


def test_ws_when_is_last_evaluation_for_output_port():
    pg = PortGraph()
    pg.add_vertex(0)
    pg.add_out_port(0, "out", 0)

    ws = WorkflowState(pg)
    ws.set_last_evaluation(0, 1)
    assert ws.when(0) == 1


def test_ws_when_raise_key_error_for_unset_param():
    pg = PortGraph()
    pg.add_vertex(0)
    pg.add_in_port(0, "in", 0)

    ws = WorkflowState(pg)
    assert_raises(KeyError, lambda: ws.when(0))


def test_ws_when_set_explicitly_for_params():
    pg = PortGraph()
    pg.add_vertex(0)
    pg.add_in_port(0, "in", 0)

    ws = WorkflowState(pg)
    ws.store_param(0, "param", 10)
    assert ws.when(0) == 10


def test_ws_when_connected_input_port():
    pg = PortGraph()
    pg.add_vertex(0)
    pg.add_out_port(0, "out", 0)
    pg.add_vertex(1)
    pg.add_out_port(1, "out", 1)
    pg.add_vertex(2)
    pg.add_in_port(2, "in", 2)
    pg.connect(0, 2)

    ws = WorkflowState(pg)
    assert ws.when(2) is None
    ws.set_last_evaluation(0, 10)
    assert ws.when(2) == 10

    pg.connect(1, 2)
    ws = WorkflowState(pg)
    assert ws.when(2) is None
    ws.set_last_evaluation(0, 10)
    assert ws.when(2) is None
    ws.set_last_evaluation(1, 11)
    assert ws.when(2) == 10


def test_ws_sub_ready_for_evaluation_if_no_input_port():
    pg = PortGraph()
    pg.add_vertex(0)
    pg.add_out_port(0, "out", 0)
    pg.add_vertex(1)
    pg.add_in_port(1, "in", 1)
    pg.connect(0, 1)

    ws = WorkflowState(pg)
    assert ws.is_ready_for_evaluation()

    subpg = get_upstream_subportgraph(pg, 1)
    subws = WorkflowState(subpg)
    assert subws.is_ready_for_evaluation()


# def test_ws_sub_ready_for_evaluation_with_input_port():
#     pg = PortGraph()
#     pg.add_vertex(0)
#     pg.add_in_port(0, "in", 0)
#     pg.add_out_port(0, "out", 1)
#     pg.add_vertex(1)
#     pg.add_in_port(1, "in", 2)
#     pg.connect(1, 2)
#
#     ws = WorkflowState(pg)
#     assert not ws.is_ready_for_evaluation()
#     ws.store_param(0, "param", 0)
#     assert ws.is_ready_for_evaluation()
#
#     subpg = get_upstream_subportgraph(pg, 2)
#     subws = WorkflowState(subpg)
#     assert subws.is_ready_for_evaluation()
