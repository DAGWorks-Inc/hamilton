# Examples for generators

## Lexicon

Comments above each function specify its role:
1. `input` -- input data to a generator
2. `expand` -- generator itself
3. `process` -- map operation on individual generator data
4. `join` -- joins the data from a generator

## Use-cases

Testing for now has just legitimate cases.

### `sequential_linear.py`

Legitimate. Basic linear case. One of each step.

### `sequential_diamond.py`

Legitimate. Edge-case, two process steps, one of each join/expand.

### `sequential_nested.py`

Potentially Legitimate (TBD). Nested case -- two expands and two joins. This is still a WIP, but we'll likely want to support this.

### `degenerate_linear.py`

Legitimate. Expand -> contract with no process steps.
