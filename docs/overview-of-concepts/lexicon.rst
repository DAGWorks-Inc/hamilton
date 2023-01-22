=======
Lexicon
=======

Before we dive into the concepts, let's clarify the terminology we'll be using

.. list-table::
   :header-rows: 0

   * - Directed Acyclic Graph (DAG)
     - A `directed acyclic graph <https://en.wikipedia.org/wiki/Directed\_acyclic\_graph>`_ is a computer science/mathematics term for representing the world with "nodes" and "edges", where "edges" only flow in one direction. It is called a graph because it can be drawn and visualized.
   * - Dataflow
     - The organization of functions and dependencies. This is a DAG -- it's directed (one function is running before the other), acyclic, (there are no cycles, i.e., no function runs before itself), and a graph (it is easily naturally represented by nodes and edges) and can be represented visually.
   * - Transform Function, or simply Function
     - A python function used to represent a Hamilton transform -- it can compile to one (in the standard case) or many (with the use of decorators) transforms. See :doc:`writing-transform-functions` for more details.
   * - Transform
     - A step in the dataflow DAG representing a computation -- usually 1:1 with functions but decorators break that pattern -- in which case multiple transforms trace back to a single function.
   * - Node
     - Synonymous with Transform. A node in the DAG, is equivalent to a transform step in the DAG. We just sometimes aren't consistent with our terminology.
   * - Hamilton Driver
     - A specific class written to enable execution of the DAG with certain parameters. This will be your main interface with your Hamilton code. See :doc:`the-hamilton-driver` for more detail.
   * - "*driver*" script/code
     - Code that you write to tell Hamilton what the DAG is, what outputs you want, and where the inputs come from.
   * - Config
     - Data that dictates the way the DAG is constructed. See :ref:`parameterizing-the-dag` for more details.
   * - Decorators
     - A python function that modifies another python function in some way. Used in Hamilton to compile functions to sets of transforms. See :doc:`decorators` for more detail.
