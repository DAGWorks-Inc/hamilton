# Get started

This accompanies the blog post on decoupling IO from transformation: https://blog.dagworks.io/p/separate-data-io-from-transformation.
To get started, you can run:

```bash
pip install -r requirements.txt
python run.py --help
Usage: run.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  run
  visualize
```

Then you can pass it a stage and mess with the code.

You'll find sample data in `data/`, and the actual hamilton functions in `components`
