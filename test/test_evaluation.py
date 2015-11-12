from nose.tools import assert_raises

from workflow.evaluation import (EvaluationError,
                                 AbstractEvaluation,
                                 BruteEvaluation,
                                 LazyEvaluation)
from workflow.evaluation_environment import EvaluationEnvironment
from workflow.func_node import RawFuncNode, FuncNode
from workflow.port_graph import PortGraph
from workflow.state import WorkflowState
# from workflow.sub_port_graph import SubPortGraph


def test_abstract_evaluation_is_abstract():
    algo = AbstractEvaluation(None)
    assert_raises(NotImplementedError, lambda: algo.requires_evaluation(0, 0))
    assert_raises(NotImplementedError, lambda: algo.eval(None, None))


def test_evaluation_new_require_evaluation():
    pg = PortGraph()
    pg.add_vertex(0)

    algo = BruteEvaluation(pg)
    assert id(algo.portgraph()) == id(pg)

    env = EvaluationEnvironment()
    ws = WorkflowState(pg)
    assert algo.requires_evaluation(env, ws)


def test_evaluation_needs_ready_to_evaluate_state():
    def func(a, b):
        c = a + b
        return c

    pg = PortGraph()
    n = FuncNode(func)
    pg.add_actor(n, 0)

    algo = BruteEvaluation(pg)

    ws = WorkflowState(pg)
    assert_raises(EvaluationError, lambda: algo.eval(None, ws))


def test_evaluation_clear():
    def func():
        pass

    pg = PortGraph()
    n = FuncNode(func)
    pg.add_actor(n, 0)
    pg.add_actor(n, 1)

    algo = BruteEvaluation(pg)

    env = EvaluationEnvironment()
    ws = WorkflowState(pg)
    algo.eval(env, ws)

    assert not algo.requires_evaluation(env, ws)

    ws.clear()

    assert algo.requires_evaluation(env, ws)


def test_evaluation_eval_all_nodes():
    visited = []

    def func():
        visited.append("yes")

    pg = PortGraph()
    n = FuncNode(func)
    pg.add_actor(n, 0)
    pg.add_actor(n, 1)

    algo = BruteEvaluation(pg)

    env = EvaluationEnvironment()
    ws = WorkflowState(pg)
    algo.eval(env, ws)

    assert len(visited) == 2


def test_evaluation_affect_output_to_right_ports():
    def func(a, b):
        c = a + b
        d = a * 2
        return c, d

    # simple order
    pg = PortGraph()
    n = FuncNode(func)
    pg.add_vertex(0)
    pg.add_in_port(0, 'a', 0)
    pg.add_in_port(0, 'b', 1)
    pg.add_out_port(0, 'c', 2)
    pg.add_out_port(0, 'd', 3)

    pg.set_actor(0, n)

    algo = BruteEvaluation(pg)
    env = EvaluationEnvironment()
    ws = WorkflowState(pg)
    ws.store_param(0, 'a', 0)
    ws.store_param(1, 'b', 0)
    algo.eval(env, ws)

    assert ws.get(2) == 'ab'
    assert ws.get(3) == 'aa'

    # reverse input orders
    pg = PortGraph()
    n = FuncNode(func)
    pg.add_vertex(0)
    pg.add_in_port(0, 'b', 0)
    pg.add_in_port(0, 'a', 1)
    pg.add_out_port(0, 'c', 2)
    pg.add_out_port(0, 'd', 3)

    pg.set_actor(0, n)

    algo = BruteEvaluation(pg)
    env = EvaluationEnvironment()
    ws = WorkflowState(pg)
    ws.store_param(0, 'a', 0)
    ws.store_param(1, 'b', 0)
    algo.eval(env, ws)

    assert ws.get(2) == 'ba'
    assert ws.get(3) == 'bb'

    # reverse output order
    pg = PortGraph()
    n = FuncNode(func)
    pg.add_vertex(0)
    pg.add_in_port(0, 'a', 0)
    pg.add_in_port(0, 'b', 1)
    pg.add_out_port(0, 'd', 2)
    pg.add_out_port(0, 'c', 3)

    pg.set_actor(0, n)

    algo = BruteEvaluation(pg)
    env = EvaluationEnvironment()
    ws = WorkflowState(pg)
    ws.store_param(0, 'a', 0)
    ws.store_param(1, 'b', 0)
    algo.eval(env, ws)

    assert ws.get(3) == 'ab'
    assert ws.get(2) == 'aa'


def test_evaluation_do_not_reevaluate_same_node():
    visited = []

    def func():
        visited.append("yes")

    pg = PortGraph()
    n = FuncNode(func)
    pg.add_actor(n, 0)
    pg.add_actor(n, 1)

    env = EvaluationEnvironment()
    algo = BruteEvaluation(pg)

    ws = WorkflowState(pg)
    algo.eval(env, ws, 0)
    assert len(visited) == 1

    algo.eval(env, ws, 1)
    assert len(visited) == 2

    algo.eval(env, ws, 0)
    assert len(visited) == 2

    env.new_execution()
    algo.eval(env, ws, 0)
    assert len(visited) == 3


def test_evaluation_propagated_upstream():
    visited = []

    def func(txt):
        visited.append(txt)
        return txt

    pg = PortGraph()
    n = FuncNode(func)
    pg.add_actor(n, 0)
    pg.add_actor(n, 1)
    pg.connect(pg.out_port(0, 'txt'), pg.in_port(1, 'txt'))

    algo = BruteEvaluation(pg)

    ws = WorkflowState(pg)
    env = EvaluationEnvironment()
    ws.store_param(pg.in_port(0, 'txt'), "txt", 0)

    algo.eval(env, ws, 0)
    assert len(visited) == 1
    algo.eval(env, ws, 1)
    assert len(visited) == 2

    env.new_execution()
    algo.eval(env, ws, 1)
    assert len(visited) == 4


def test_evaluation_fail_if_function_returns_single_value():
    def func():
        return 1

    pg = PortGraph()
    pg.add_vertex(0)
    pg.add_out_port(0, 'res', 0)
    n = RawFuncNode(func)
    n.add_output('res', "descr")
    pg.set_actor(0, n)

    algo = BruteEvaluation(pg)

    env = EvaluationEnvironment()
    ws = WorkflowState(pg)
    assert_raises(EvaluationError, lambda: algo.eval(env, ws))


def test_evaluation_fail_if_port_mismatch_outputs():
    def func():
        return 1, 2

    pg = PortGraph()
    pg.add_vertex(0)
    pg.add_out_port(0, 'res', 0)
    n = RawFuncNode(func)
    n.add_output('res', "descr")
    pg.set_actor(0, n)

    algo = BruteEvaluation(pg)

    env = EvaluationEnvironment()
    ws = WorkflowState(pg)
    assert_raises(EvaluationError, lambda: algo.eval(env, ws))


###############################################################################
#
#   lazy evaluation
#
###############################################################################

def test_lazy_evaluate_at_least_once_each_node():
    evaluated = []

    def func():
        evaluated.append('bla')

    pg = PortGraph()
    vid = pg.add_actor(FuncNode(func))
    assert pg.actor(vid).is_lazy()

    algo = LazyEvaluation(pg)
    env = EvaluationEnvironment()
    ws = WorkflowState(pg)

    algo.eval(env, ws)
    assert len(evaluated) == 1
    algo.eval(env, ws)
    assert len(evaluated) == 1


def test_lazy_do_not_reevaluate_node_if_same_inputs():
    evaluated = []

    def func(txt):
        evaluated.append(txt)
        return txt

    pg = PortGraph()
    vid = pg.add_actor(FuncNode(func))
    assert pg.actor(vid).is_lazy()

    algo = LazyEvaluation(pg)
    env = EvaluationEnvironment()
    ws = WorkflowState(pg)
    ws.store_param(pg.in_port(vid, 'txt'), 'toto', env.current_execution())

    algo.eval(env, ws)
    assert len(evaluated) == 1

    env.new_execution()
    algo.eval(env, ws)
    assert len(evaluated) == 1


def test_lazy_always_reevaluate_non_lazy_nodes():
    evaluated = []

    def func(txt):
        evaluated.append(txt)
        return txt

    pg = PortGraph()
    vid = pg.add_actor(FuncNode(func))
    pg.actor(vid).set_lazy(False)

    algo = LazyEvaluation(pg)
    env = EvaluationEnvironment()
    ws = WorkflowState(pg)
    ws.store_param(pg.in_port(vid, 'txt'), 'toto', env.current_execution())

    algo.eval(env, ws)
    assert len(evaluated) == 1

    env.new_execution()
    algo.eval(env, ws)
    assert len(evaluated) == 2


def test_lazy_always_reevaluate_if_inputs_changed():
    evaluated = []

    def func(txt):
        evaluated.append(txt)
        return txt

    pg = PortGraph()
    vid = pg.add_actor(FuncNode(func))
    assert pg.actor(vid).is_lazy()

    algo = LazyEvaluation(pg)
    env = EvaluationEnvironment()
    ws = WorkflowState(pg)
    ws.store_param(pg.in_port(vid, 'txt'), 'toto', env.current_execution())

    algo.eval(env, ws)
    assert len(evaluated) == 1

    env.new_execution()
    ws.store_param(pg.in_port(vid, 'txt'), 'toto', env.current_execution())
    algo.eval(env, ws)
    assert len(evaluated) == 2


def test_lazy_do_not_reevaluate_node_if_same_execution():
    evaluated = []

    def func(txt):
        evaluated.append(txt)
        return txt

    pg = PortGraph()
    vid = pg.add_actor(FuncNode(func))
    assert pg.actor(vid).is_lazy()

    algo = LazyEvaluation(pg)
    env = EvaluationEnvironment()
    ws = WorkflowState(pg)
    ws.store_param(pg.in_port(vid, 'txt'), 'toto', env.current_execution())

    algo.eval(env, ws)
    assert len(evaluated) == 1

    algo.eval_node(env, ws, vid)
    assert len(evaluated) == 1
