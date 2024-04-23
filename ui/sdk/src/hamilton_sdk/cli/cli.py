import os

import click
from hamilton_sdk.cli import initialize


@click.group()
def cli():
    pass


@click.command()
@click.option("--api-key", "-k", required=True, type=str)
@click.option("--username", "-u", required=True, type=str)
@click.option("--project-id", "-p", required=True, type=int)
@click.option("--template", "-t", required=False, type=click.Choice(initialize.TEMPLATES))
@click.option("--location", "-l", type=click.Path(exists=False, dir_okay=True), default=None)
def init(api_key: str, username: str, project_id: int, template: str, location: str):
    if location is None:
        # If location is none we default to, say, ./hello_world
        location = os.path.join(os.getcwd(), template)
    initialize.generate_template(
        username=username,
        api_key=api_key,
        project_id=project_id,
        template=template,
        copy_to_location=location,
    )


cli.add_command(init)

if __name__ == "__main__":
    cli()
