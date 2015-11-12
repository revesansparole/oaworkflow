""" FuncNode class

A FuncNode is a Node whose __call__ method
actually use an external function
"""

import ast
import inspect

from node import Node


def argtype(*arg_types_list):
    def type_decorator(func):
        func.__argtypes__ = arg_types_list
        return func

    return type_decorator


def rettype(*ret_types_list):
    def type_decorator(func):
        func.__rettypes__ = ret_types_list
        return func

    return type_decorator


def elm_to_name(elm):
    if isinstance(elm, ast.Name):
        return elm.id
    else:
        return "res"


def ensure_unique(elms):
    uelms = [None] * len(elms)
    inds = range(len(elms))
    while len(inds) > 0:
        elm = elms[inds[0]]
        if elms.count(elm) == 1:
            uelms[inds.pop(0)] = elm
        else:
            i = -1
            for cu in range(elms.count(elm)):
                i = elms.index(elm, i + 1)
                inds.remove(i)
                uelms[i] = "%s%d" % (elm, cu + 1)

    return uelms


class RawFuncNode(Node):
    """A FuncNode is a Node whose __call__ method
    actually use an external function
    """

    def __init__(self, func):
        """ Default constructor
        """
        Node.__init__(self)

        if func is None:
            raise TypeError("func must be callable")

        if not callable(func):
            raise TypeError("func must be callable")

        self._id = ":".join((inspect.getmodule(func).__name__, func.__name__))
        self._func = func

    def __call__(self, inputs=()):
        return self._func(*inputs)

    def reset(self):
        pass


class FuncNode(RawFuncNode):
    """A FuncNode is a RawFuncNode whose inputs
    and outputs are automatically filled.

    Works only for well defined functions:
     - no *args
     - no **kwds
     - no strange return
     - e.g. def func(a, b=3, c='a'):
                res = a + b
                return c, res
    """

    def __init__(self, func):
        """ Default Constructor
        """
        RawFuncNode.__init__(self, func)

        args, varargs, keywords, defaults = inspect.getargspec(func)
        if varargs is not None:
            msg = "function must not have *args, use RawFuncNode instead"
            raise TypeError(msg)

        if keywords is not None:
            msg = "function must not have **kwds, use RawFuncNode instead"
            raise TypeError(msg)

        if hasattr(func, '__argtypes__'):
            argtypes = func.__argtypes__
        else:
            argtypes = [None] * len(args)

        for name, typ in zip(args, argtypes):
            self.add_input(name, typ, None, "None")

        self._output_type = "None"

        pycode = inspect.getsource(func)

        # remove blanks at the beginning of lines for
        # functions defined inside other objects
        nb_spaces = 0
        while pycode[nb_spaces] in (" ", "\n"):
            nb_spaces += 1

        pycode = "\n".join([line[nb_spaces:] for line in pycode.splitlines()])

        # coarse find return line
        ct = ast.parse(pycode)
        fd = ct.body[0]
        ret = fd.body[-1]
        if isinstance(ret, ast.Return):
            if isinstance(ret.value, ast.Tuple):
                self._output_type = "tuple"

                rets = [elm_to_name(elm) for elm in ret.value.elts]
                if hasattr(func, '__rettypes__'):
                    rettypes = func.__rettypes__
                else:
                    rettypes = [None] * len(rets)

                for name, typ in zip(ensure_unique(rets), rettypes):
                    self.add_output(name, typ, None, "None")
            else:
                self._output_type = "single"
                name = elm_to_name(ret.value)
                if hasattr(func, '__rettypes__'):
                    typ = func.__rettypes__[0]
                else:
                    typ = None
                self.add_output(name, typ, None, "None")

    def __call__(self, inputs=()):
        ret = self._func(*inputs)
        if self._output_type == 'None':
            return ()
        elif self._output_type == 'single':
            return ret,
        else:
            return ret
