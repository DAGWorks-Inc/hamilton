import sys

import click

from hamilton.migration import compilation


@click.group('main')
def main():
    pass


@main.command()
@click.argument('input', type=click.Path(exists=True))
@click.argument('output', type=click.Path(exists=False), required=False, default=None)
def translate(input: str, output: str):
    if not input.endswith('.py'):
        raise ValueError(f"File to convert must be a standard python file. {input} is not.")
    if output is not None and not output.endswith('.py'):
        raise ValueError(f"File to write to must be a standard python file. {output} is not.")
    with open(input, 'r') as f_r:
        raw_contents = f_r.read()
        transpiled_contents = compilation.transpile_contents(raw_contents)
    with open(output, 'w') if output is not None else sys.stdout as f_w:
        f_w.write(transpiled_contents)


if __name__ == '__main__':
    main()
