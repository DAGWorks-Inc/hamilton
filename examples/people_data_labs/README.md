# People Data Labs

[People Data Labs](https://www.peopledatalabs.com/) is a data provider that offers several data APIs for person enrichment & search, company enrichment & search, and IP enrichment.

This example showcases how Hamilton can help you write modular data transformations.


## Content
- `notebook.ipynb` is a step-by-step introduction to Hamilton. Start there.
- `analysis.py` contains data transformations used by `run.py`. They're the same as the ones defined in `notebook.ipynb`.
- `run.py` contains code to execute the analysis.


## Set up
1. Create a virtual environment and activate it
    ```console
    python -m venv venv && . venv/bin/active
    ```

2. Install requirements
    ```console
    pip install -r requirements.txt
    ```

3. Download the data with this command (it should take ~3mins)
    ```console
    python download_data.py
    ```

4. Visit the notebook or execute the script
    ```console
    python run.py
    ```

## Resources
- [PDL Blog](https://blog.peopledatalabs.com/) and [PDL Recipes](https://docs.peopledatalabs.com/recipes)
- [Interactive Hamilton training](https://www.tryhamilton.dev/hamilton-basics/jumping-in)
- [Hamilton documentation](https://hamilton.dagworks.io/en/latest/concepts/node/)
- more [Hamilton code examples](https://github.com/DAGWorks-Inc/hamilton/tree/main/examples) and integrations with Python tools.
