from nose.tools import assert_raises

from workflow.node import Node


def test_node_creation():
    n = Node()
    assert n.get_id() == "openalea.workflow.node:Node"

    assert_raises(NotImplementedError, lambda: n.reset())
    assert_raises(NotImplementedError, lambda: n())


def test_node_ports_are_empty_on_creation():
    n = Node()
    assert len(list(n.inputs())) == 0
    assert len(list(n.outputs())) == 0


def test_node_port_keys_are_locally_unique():
    n = Node()
    n.add_input(1, "titi")
    assert len(list(n.inputs())) == 1
    n.add_input("toto", "titi")
    assert len(list(n.inputs())) == 2

    assert_raises(KeyError, lambda: n.add_input(1, "toto"))

    n.add_output(1, "tata")
    assert len(list(n.outputs())) == 1
    n.add_output("toto", "tata")
    assert len(list(n.outputs())) == 2

    assert_raises(KeyError, lambda: n.add_output("toto", "toto"))


def test_node_ports_order_is_preserved():
    n = Node()
    n.add_output('a1', "descr")
    n.add_output('a2', "descr")
    n.add_output('res', "descr")
    assert tuple(n.outputs()) == ('a1', 'a2', 'res')


def test_node_port_type():
    n = Node()
    n.add_input("in", "IInt")
    n.add_output("out", "IFloat")

    assert_raises(KeyError, lambda: n.input("toto"))
    assert_raises(KeyError, lambda: n.output("toto"))

    assert n.input("in").type == "IInt"
    assert n.output("out").type == "IFloat"


def test_node_port_default_value():
    n = Node()
    n.add_input("in1", "IInt")
    n.add_input("in2", "IInt", 0)
    n.add_input("in3", default='a')
    n.add_output("out", "IFloat")

    assert_raises(KeyError, lambda: n.input("toto"))
    assert_raises(KeyError, lambda: n.output("toto"))

    assert n.input("in1").default is None
    assert n.input("in2").default == 0
    assert n.input("in3").default == 'a'
    assert n.output("out").type == "IFloat"


def test_node_port_descr():
    n = Node()
    n.add_input(1, descr="titi")
    n.add_output(2, descr="titi")

    assert n.input(1).descr == "titi"
    assert_raises(KeyError, lambda: n.input("toto"))

    assert n.output(2).descr == "titi"
    assert_raises(KeyError, lambda: n.output(1))


def test_node_port_is_mutable():
    n = Node()
    n.add_input(1)
    n.add_output(1)
    n.input(1).type = "IInt"
    n.input(1).default = 0
    n.input(1).descr = "toto"

    n.output(1).type = "IFloat"
    n.output(1).default = 'a'
    n.output(1).descr = "titi"

    assert n.input(1).type == "IInt"
    assert n.input(1).default == 0
    assert n.input(1).descr == "toto"

    assert n.output(1).type == "IFloat"
    assert n.output(1).default == 'a'
    assert n.output(1).descr == "titi"


def test_node_is_lazy_by_default():
    n = Node()
    assert n.is_lazy()

    n.set_lazy(True)
    assert n.is_lazy()
    n.set_lazy(False)
    assert not n.is_lazy()
    n.set_lazy(True)
    assert n.is_lazy()


def test_node_priority():
    n = Node()
    assert n.priority() == 0

    n.set_priority(1)
    assert n.priority() == 1

    assert_raises(TypeError, lambda: n.set_priority(1.1))
    assert_raises(TypeError, lambda: n.set_priority("bcp"))


def test_node_caption():
    n = Node()
    assert n.caption() == "caption"

    n.set_caption("toto")
    assert n.caption() == "toto"
    n.set_caption(1)
    assert n.caption() == "1"
