# Lexicon

Before we dive into the concepts, let's clarify the terminology we'll be using

|            |                                                                                                                                                                                                                                           |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Function   | A python function used to represent a hamilton transform -- can compile to one (in the standard case) or many (with the use of decorators). See [writing transforms](writing-transforms.md) for more details.                             |
| Transform  | A node in the DAG -- usually 1:1 with functions but decorators break that pattern.                                                                                                                                                        |
| Dataflow   | The organization of functions and dependencies. This is a DAG -- it's directed (one function is running before the other), acyclic, (no function runs before itself), and a graph (it is easily naturally represented by nodes and edges) |
| Driver     | A class written to enable execution of the DAG with certain parameters. See [drivers](drivers.md) for more detail.                                                                                                                        |
| Config     | Data that dictates the way the DAG is constructed. See [parametrizing the dag](parametrizing-the-dag.md) for more details.                                                                                                                |
| Decorators | A python function that modifies another python function in some way. Used in Hamilton to compile functions to sets of transforms. See [decorators](decorators.md) for more detail.                                                        |
