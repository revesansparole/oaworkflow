from nose.tools import assert_raises

from openalea.workflow.func_node import RawFuncNode, FuncNode, argtype, rettype


def test_raw_func_node_func_is_callable():
    assert_raises(TypeError, lambda: RawFuncNode(None))
    assert_raises(TypeError, lambda: RawFuncNode("toto"))
    assert_raises(TypeError, lambda: RawFuncNode(1))


def test_raw_func_node_id_is_unique_per_function():
    def toto1():
        return 1

    def toto2():
        return 2

    n1 = RawFuncNode(toto1)
    n2 = RawFuncNode(toto2)

    assert n1.get_id() != n2.get_id()
    assert n1.get_id() == RawFuncNode(toto1).get_id()


def test_raw_func_node_return_value():
    def func():
        pass

    n = RawFuncNode(func)
    assert n() is None
    assert_raises(TypeError, lambda: n((1, 2)))

    def func():
        return 1

    n = RawFuncNode(func)
    assert n() == 1
    assert n(()) == 1

    def func(a, *args):
        return a + sum(args)

    n = RawFuncNode(func)
    assert n((1, 2, 3)) == 6


def test_raw_func_node_reset_is_doing_nothing():
    def func():
        pass

    n = RawFuncNode(func)
    assert n.reset() is None


def test_func_node_inline():
    def tata(a, b):
        c = a + b
        return c

    n = FuncNode(tata)
    assert n((1, 2)) == (3, )


def test_func_node_no_args_or_kwds():
    def func1(a, *args):
        return None

    assert_raises(TypeError, lambda: FuncNode(func1))

    def func2(**kwds):
        return None

    assert_raises(TypeError, lambda: FuncNode(func2))


def test_func_node_args_name_detection():
    def func():
        return None

    n = FuncNode(func)
    assert tuple(n.inputs()) == ()

    def func(a, b):
        return None

    n = FuncNode(func)
    assert tuple(n.inputs()) == ('a', 'b')


def test_func_node_args_type_detection():
    def func(a):
        return a

    n = FuncNode(func)
    assert n.input('a').type is None

    def func(a):
        return a

    func.__argtypes__ = ["IInt"]
    n = FuncNode(func)
    assert n.input('a').type == "IInt"

    @argtype("IInt")
    def func(a):
        return a

    n = FuncNode(func)
    assert n.input('a').type == "IInt"

    @argtype("IInt", "IFloat")
    def func(a, b):
        return a + b

    n = FuncNode(func)
    assert n.input('a').type == "IInt"
    assert n.input('b').type == "IFloat"


def test_func_node_return_outputs():
    def func():
        return 1

    n = FuncNode(func)
    assert tuple(n.outputs()) == ('res',)

    def func():
        a = 1
        return a

    n = FuncNode(func)
    assert tuple(n.outputs()) == ('a',)

    def func():
        a = 1
        return a, 1

    n = FuncNode(func)
    assert tuple(n.outputs()) == ('a', 'res')


def test_func_node_return_none_differ_from_no_return():
    def func():
        return None

    n = FuncNode(func)
    assert tuple(n.outputs()) == ('None',)

    def func():
        print None

    n = FuncNode(func)
    assert tuple(n.outputs()) == ()


def test_func_node_output_names_are_unique():
    def func():
        a = 1
        return a, a, 2

    n = FuncNode(func)
    assert tuple(n.outputs()) == ('a1', 'a2', 'res')


def test_func_node_return_type_detection():
    def func(a):
        return a

    n = FuncNode(func)
    assert n.output('a').type is None

    def func(a):
        return a

    func.__rettypes__ = ["IInt"]
    n = FuncNode(func)
    assert n.output('a').type == "IInt"

    @rettype("IInt")
    def func(a):
        return a

    n = FuncNode(func)
    assert n.output('a').type == "IInt"

    @rettype("IInt", "IFloat")
    def func(a, b):
        return a, b

    n = FuncNode(func)
    assert n.output('a').type == "IInt"
    assert n.output('b').type == "IFloat"


def test_func_node_return_value():
    # no outputs
    def func():
        pass

    n = FuncNode(func)
    assert n() == ()

    # function explicitly returns None
    def func(a, b):
        return None

    n = FuncNode(func)
    assert n((1, 2)) == (None,)

    # single value output
    def func():
        return 1

    n = FuncNode(func)
    assert n() == (1,)

    def func():
        tup = (1, 2)
        return tup

    n = FuncNode(func)
    assert n() == ((1, 2),)

    # multi outputs
    def func():
        return 'a', 1

    n = FuncNode(func)
    assert n() == ('a', 1)
