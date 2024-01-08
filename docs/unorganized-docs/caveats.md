# Caveats

## Delayed evaluation of annotation

Hamilton works with [PEP-563](https://peps.python.org/pep-0563/), postponed evaluation of annotations.
That said, it *does* force evaluation of type-hints when building the function graph. So, if you're using
particularly complex/slow to load python types and expecting delay, know that they will have to be evaluated
when the driver is instantiated and modules are passed, so Hamilton can inspect the types and build the
function graph.
