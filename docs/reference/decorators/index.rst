===========
Decorators
===========

While the 1:1 mapping of output -> function implementation is powerful, we've implemented a few decorators to promote
business-logic reuse, as well as to layer on extra capabilities. Source for these decorators can be found in the
`function_modifiers module <https://github.com/dagworks-inc/hamilton/blob/main/hamilton/function_modifiers>`__.

For reference we list available decorators for Hamilton here. Note: use
``from hamilton.function_modifiers import DECORATOR_NAME`` to use these decorators:

.. toctree::
   :maxdepth: 2

   check_output
   config_when
   does
   extract_columns
   extract_fields
   inject
   load_from
   parameterize
   parameterize_extract_columns
   parameterize_frame
   parameterize_sources
   parameterize_subdag
   parameterize_values
   resolve
   save_to
   subdag
   tag
