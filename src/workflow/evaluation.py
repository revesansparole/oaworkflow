""" This module provide algorithms to evaluate a portgraph
"""

# TODO: remove portgraph attribute


class EvaluationError(Exception):
    pass


class AbstractEvaluation(object):
    """ Abstract evaluation algorithm
    """
    def __init__(self, portgraph):
        """ Constructor

        args:
            - portgraph (PortGraph): the portgraph to evaluate
        """
        self._portgraph = portgraph

    def portgraph(self):
        """ Testing purpose, retrieve associated portgraph
        """
        return self._portgraph

    def requires_evaluation(self, env, state):
        """ Return True if the associated portgraph need to be evaluated.

        args:
         - env (EvaluationEnvironment): environment in which to perform
                                        the evaluation
         - state (WorkflowState): current state of workflow
        """
        raise NotImplementedError()

    def eval(self, env, state, vid=None):
        """ Evaluate associated portgraph.

        Produce a valid state from a ready_to_evaluate one.

        args:
            - env (EvaluationEnvironment): environment in which to perform
                                           the evaluation
            - state (PortGraphState): must be a ready_to_evaluate state
            - vid (vid): id of vertex to start the evaluation
                         if None starts from the leaves of the portgraph
        """
        raise NotImplementedError()


class BruteEvaluation(AbstractEvaluation):
    """ For each evaluation reevaluate each node of the portgraph.
    """
    def __init__(self, portgraph):
        AbstractEvaluation.__init__(self, portgraph)

    def requires_evaluation(self, env, state):
        current_eid = env.current_execution()

        for vid in self._portgraph.vertices():
            if state.last_evaluation(vid) != current_eid:
                return True

        return False

    def eval(self, env, state, vid=None):
        pg = self._portgraph

        if not state.is_ready_for_evaluation():
            raise EvaluationError("state not ready for evaluation")

        current_eid = env.current_execution()
        if vid is None:  # start evaluation from leaves in the portgraph
            leaves = [v for v in pg.vertices() if pg.nb_out_edges(v) == 0]
            leaves = [(pg.actor(v).priority(), v) for v in leaves]
            leaves.sort(reverse=True)
            leaves = [v for priority, v in leaves]

            for vid in leaves:
                if state.last_evaluation(vid) != current_eid:
                    self.eval_from_node(env, state, vid)
        else:
            if state.last_evaluation(vid) != current_eid:
                self.eval_from_node(env, state, vid)

    def eval_from_node(self, env, state, vid):
        """ Evaluate portgraph from a given node.

        function provided for convenience to simplify
        derivation from this algo
        """
        current_eid = env.current_execution()

        # ensure that all nodes upstream of this node have been evaluated
        for nid in self._portgraph.in_neighbors(vid):
            if state.last_evaluation(nid) != current_eid:
                self.eval_from_node(env, state, nid)

        # evaluate the node
        self.eval_node(env, state, vid)

    def eval_node(self, env, state, vid):
        """ Evaluate a single node

        Store result in state.
        Doesn't test if state is valid or if the node
        actually needs to be evaluated
        """
        pg = self._portgraph
        node = pg.actor(vid)

        # find input values
        # match node input keys to portgraph in ports
        inputs = []
        for key in node.inputs():
            pid = pg.in_port(vid, key)
            inputs.append(state.get(pid))

        # perform computation
        state.set_last_evaluation(vid, env.current_execution())
        values = node(inputs)

        # affect return values to output ports
        # match node output keys to portgraph out ports
        outputs = tuple(node.outputs())
        try:
            if len(outputs) != len(values):
                msg = "mismatch nb out ports vs. function result"
                raise EvaluationError(msg)
            else:
                for key, val in zip(outputs, values):
                    pid = pg.out_port(vid, key)
                    state.store(pid, val)
        except TypeError:
            msg = "Function needs to return a list of values"
            raise EvaluationError(msg)


class LazyEvaluation(BruteEvaluation):
    """ For each evaluation reevaluate a node of the dataflow
    only if its inputs have changed or if it is tagged
    as not lazy.
    """
    def __init__(self, portgraph):
        BruteEvaluation.__init__(self, portgraph)

    def eval_node(self, env, state, vid):
        """ Evaluate a single node

        call BruteEvaluation:
            - if node is not lazy
            - if node is lazy but inputs have changed
        """
        if state.last_evaluation(vid) is None:
            return BruteEvaluation.eval_node(self, env, state, vid)
        elif state.last_evaluation(vid) == env.current_execution():
            # node has already been evaluated at this execution
            # do nothing
            pass
        else:
            pg = self._portgraph
            node = pg.actor(vid)

            if node.is_lazy():
                # re evaluate only if inputs have changed after
                # last evaluation
                eid = state.last_evaluation(vid)
                if any(state.when(pid) > eid for pid in pg.in_ports(vid)):
                    return BruteEvaluation.eval_node(self, env, state, vid)
            else:
                return BruteEvaluation.eval_node(self, env, state, vid)
