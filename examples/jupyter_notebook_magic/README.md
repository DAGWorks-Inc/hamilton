# This example shows a notebook using the Hamilton Jupyter magic

To load the magic:
```python
# load some extensions / magic...
%load_ext hamilton.plugins.jupyter_magic
```

Then to use it:

```python
%%cell_to_module -m MODULE_NAME # more args
```
Other arguments (--help to print this.):
  -m, --module_name: Module name to provide. Default is jupyter_module.
  -c, --config: JSON config string, or variable name containing config to use.
  -r, --rebuild-drivers: Flag to rebuild drivers.
  -d, --display: Flag to visualize dataflow.
  -v, --verbosity: of standard output. 0 to hide. 1 is normal, default.

Example use:

```python
%%cell_to_module -m MODULE_NAME --display --rebuild-drivers
```
