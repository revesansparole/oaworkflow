""" This module provide data structure to store global parameters
for a dataflow evaluation.
"""

from openalea.core.graph.id_generator import IdGenerator


class EvaluationEnvironment(object):
    """ Environment for evaluation algorithms
    """
    def __init__(self, exec_id=None):
        """ Constructor

        args:
            - exec_id (eid) default None:
                        current id to set for this environment
                        if None, a new one will be created
        """
        self._id_gen = IdGenerator()

        self._exec_id = self._id_gen.get_id(exec_id)

    def clear(self):
        """ Clear environment
        """
        self._id_gen = IdGenerator()
        self._exec_id = self._id_gen.get_id()

    def current_execution(self):
        """ Return id of current execution.
        """
        return self._exec_id

    def new_execution(self, exec_id=None, rel_type='>'):
        """ Change execution id to a new unused id.

        arg:
            - exec_id (eid): id of parent execution
            - rel_type ('>', '+'): type of relation with parent execution
        """
        self._exec_id = self._id_gen.get_id()

        return self._exec_id
