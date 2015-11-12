from random import random

from workflow.evaluation import LazyEvaluation
from workflow.evaluation_environment import EvaluationEnvironment
from workflow.func_node import FuncNode
from workflow.port_graph import PortGraph
from workflow.state import WorkflowState


def rfunc():
    n = int(random() * 100)
    return n


rnode = FuncNode(rfunc)
rnode.set_lazy(False)


def add(a, b):
    r = a + b
    return r


def num(n):
    return n


pg = PortGraph()
ip = pg.in_port
op = pg.out_port

pg.add_actor(rnode, 0)
pg.add_actor(FuncNode(num), 1)
pg.add_actor(FuncNode(add), 2)

pg.connect(op(0, 'n'), ip(2, 'a'))
pg.connect(op(1, 'n'), ip(2, 'b'))

env = EvaluationEnvironment()
ws = WorkflowState(pg)


def summary():
    pid = ip(1, 'n')
    print 'param', 1, 'n', ws.get(pid), ws.when(pid)

    for k in [(0, 'n'), (1, 'n'), (2, 'r')]:
        pid = op(*k)
        print k, ws.when(pid), ws.get(pid)


ws.store_param(ip(1, 'n'), 1, env.current_execution())
algo = LazyEvaluation(pg)

print "\n" * 2, "first exec", env.current_execution()
algo.eval(env, ws)
summary()
print "\n" * 2, "same exec", env.current_execution()
algo.eval(env, ws)
summary()

env.new_execution()

print "\n" * 2, "new exec", env.current_execution()
algo.eval(env, ws)
summary()

env.new_execution()

print "\n" * 2, "new exec", env.current_execution()
algo.eval(env, ws)
summary()

# print "\n" * 2, "modified param same exec", env.current_execution(), "not to do"
# ws.store_param(ip(1, 'n'), 10, env.current_execution())
# algo.eval(env, ws)
# summary()
#
# env.new_execution()
#
# print "\n" * 2, "new exec", env.current_execution()
# algo.eval(env, ws)
# summary()

env.new_execution()
ws.store_param(ip(1, 'n'), 10, env.current_execution())
print "\n" * 2, "modified param new exec", env.current_execution()

# env.new_execution()
algo.eval(env, ws)
summary()
