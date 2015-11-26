# -*- python -*-
#
#       OpenAlea.Core
#
#       Copyright 2006-2009 INRIA - CIRAD - INRA
#
#       File author(s): Jerome Chopard <revesansparole@gmail.com>
#                       Christophe Pradal <christophe.prada@cirad.fr>
#
#       Distributed under the CeCILL-C License.
#       See accompanying file LICENSE.txt or copy at
#           http://www.cecill.info/licences/Licence_CeCILL-C_V1-en.html
#
#       OpenAlea WebSite : http://openalea.gforge.inria.fr
#
###############################################################################
""" Node classes.

A Node is a generalized functor which is embedded in a workflow.
"""

from collections import namedtuple, OrderedDict


class Port(object):
    """ A simple container for information associated to
    a port of a node.
    """
    def __init__(self, type, default, descr):
        self.type = type
        self.default = default
        self.descr = descr


class Node(object):
    """
    An AbstractNode is the atomic entity in a dataflow.
    """
    _id = "openalea.workflow.node:Node"  # must be unique

    def __init__(self):
        """ Default Constructor
        """
        self._inputs = OrderedDict()
        self._outputs = OrderedDict()

        self._lazy = True
        self._priority = 0
        self._caption = "caption"

    def get_id(self):
        """ Construct a unique id based on:
        package.module:local_id

        Return:
          - (str)
        """
        return self._id

    #################################################
    #
    #   IO ports
    #
    #################################################
    def inputs(self):
        """ Iterate on ordered ids of all input ports

        Returns:
          - (iter of pid)
        """
        return self._inputs.keys()

    def input(self, key):
        """ Fetch information associated to a given
        input port.

        args:
          - key (str): id of input port

        returns:
          - (str, any, str): type, default, descr
        """
        return self._inputs[key]

    def outputs(self):
        """ Iterate on ordered ids of all output ports

        Returns:
          - (iter of pid)
        """
        return self._outputs.keys()

    def output(self, key):
        """ Fetch information associated to a given
        output port.

        args:
          - key (str): id of output port

        returns:
          - (str, any, str): type, default, descr
        """
        return self._outputs[key]

    def add_input(self, key, type="any", default=None, descr="descr"):
        """ Add an input port to this node

        Args:
          - key (pid): id of this new port, must be unique
                       among input ports of this node
          - descr (str): type of data expected on this port
        """
        if key in self._inputs:
            raise KeyError("Input '%s' already exists" % key)

        self._inputs[key] = Port(type, default, descr)

    def add_output(self, key, type="any", default=None, descr="descr"):
        """ Add an output port to this node

        Args:
          - key (pid): id of this new port, must be unique
                       among output ports of this node
          - descr (str): type of data expected on this port
        """
        if key in self._outputs:
            raise KeyError("Output '%s' already exists" % key)

        self._outputs[key] = Port(type, default, descr)

    #################################################
    #
    #   Evaluation
    #
    #################################################
    def reset(self):
        """ Restore initial computational state of this node.
        """
        raise NotImplementedError()

    def __call__(self, inputs=()):
        """ Call function. Must be overridden
        """
        raise NotImplementedError()

    #################################################
    #
    #   Attributes
    #
    #################################################
    def is_lazy(self):
        """ Check if the node allows lazy evaluation.
        """
        return self._lazy

    def set_lazy(self, flag):
        """ Set the node to allow lazy evaluation.

        args:
            - flag (bool)
        """
        self._lazy = flag

    def priority(self):
        """ Fetch priority of this node.

        Highest priority nodes at the same level
        are evaluated first.

        Returns:
          - (int)
        """
        return self._priority

    def set_priority(self, priority):
        """ Declare the priority of this node.

        Args:
          - priority (int): Highest priority nodes
             at the same level are evaluated first.
        """
        if not isinstance(priority, int):
            raise TypeError("priority must be an integer: '%s'" % priority)

        self._priority = priority

    def caption(self):
        """ Retrieve some text associated with this node.

        Return:
          - (str)
        """
        return self._caption

    def set_caption(self, caption):
        """ Set the caption associated with this node.

        Args:
          - caption (str): text
        """
        self._caption = str(caption)
