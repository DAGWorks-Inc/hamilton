import logging
from pathlib import Path
from pprint import pprint
from typing import Annotated, Any, Optional

import typer
from pydantic import BaseModel

from hamilton import driver
from hamilton.cli import commands

logger = logging.getLogger(__name__)


class Response(BaseModel):
    command: str
    success: bool
    message: Any


class CliState:
    verbose: Optional[bool] = None
    json_out: Optional[bool] = (None,)
    dr: Optional[driver.Driver] = None
    dataflow_version: Optional[dict] = None


cli = typer.Typer()
state = CliState()


# entrypoint for `hamilton` without command
@cli.callback()
def main(
    verbose: Annotated[bool, typer.Option(help="Output all intermediary commands")] = False,
    json_out: Annotated[
        bool, typer.Option(help="Output JSON for programmatic use (e.g., CI)")
    ] = False,
):
    """Hamilton CLI"""
    state.verbose = verbose
    state.json_out = json_out


@cli.command()
def build(
    ctx: typer.Context,
    modules: Annotated[
        list[Path], typer.Argument(exists=True, dir_okay=False, readable=True, resolve_path=True)
    ],
):
    """Build dataflow with MODULES"""
    try:
        config = dict()
        state.dr = commands.build(modules=modules, config=config)
    except Exception as e:
        response = Response(
            command="build", success=False, message={"error": str(type(e)), "details": str(e)}
        )
        print(response.model_dump_json())
        raise typer.Exit(code=1)

    response = Response(
        command="build", success=True, message={"modules": [p.stem for p in modules]}
    )

    if (ctx.info_name == "build") or state.verbose:
        if state.json_out is True:
            print(response.model_dump_json())
        else:
            pprint(response.message)


@cli.command()
def diff(
    ctx: typer.Context,
    modules: Annotated[
        list[Path], typer.Argument(exists=True, dir_okay=False, readable=True, resolve_path=True)
    ],
    git_reference: str = "HEAD",
    view: bool = False,
):
    """Diff between the current MODULES and their specified GIT_REFERENCE"""
    if state.dr is None:
        ctx.invoke(version, ctx=ctx, modules=modules)

    try:
        diff = commands.diff(
            dr=state.dr,
            modules=modules,
            git_reference=git_reference,
            view=view,
            config=None,
        )
    except Exception as e:
        response = Response(
            command="diff", success=False, message={"error": str(type(e)), "details": str(e)}
        )
        print(response.model_dump_json())
        raise typer.Exit(code=1)

    response = Response(
        command="diff",
        success=True,
        message=diff,
    )

    if (ctx.info_name == "diff") or state.verbose:
        if state.json_out is True:
            print(response.model_dump_json())
        else:
            pprint(response.message)


@cli.command()
def version(
    ctx: typer.Context,
    modules: Annotated[
        list[Path], typer.Argument(exists=True, dir_okay=False, readable=True, resolve_path=True)
    ],
):
    """Version NODES and DATAFLOW from dataflow with MODULES"""
    if state.dr is None:
        ctx.invoke(build, ctx=ctx, modules=modules)

    try:
        dataflow_version = commands.version(state.dr)
    except Exception as e:
        response = Response(
            command="version", success=False, message={"error": str(type(e)), "details": str(e)}
        )
        print(response.model_dump_json())
        raise typer.Exit(code=1)

    response = Response(
        command="version",
        success=True,
        message=dataflow_version,
    )
    if (ctx.info_name == "version") or state.verbose:
        if state.json_out is True:
            print(response.model_dump_json())
        else:
            pprint(response.message)


@cli.command()
def view(
    ctx: typer.Context,
    modules: Annotated[
        list[Path], typer.Argument(exists=True, dir_okay=False, readable=True, resolve_path=True)
    ],
    output_file_path: Annotated[
        Path,
        typer.Option(
            "--output",
            "-o",
            exists=False,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ] = "./dag.png",  # TODO fix default value and relative path
):
    """Build and visualize dataflow with MODULES"""
    if state.dr is None:
        # TODO add config
        ctx.invoke(build, ctx=ctx, modules=modules)

    try:
        commands.view(dr=state.dr, output_file_path=output_file_path)
    except Exception as e:
        response = Response(
            command="view", success=False, message={"error": str(type(e)), "details": str(e)}
        )
        print(response.model_dump_json())
        raise typer.Exit(code=1)

    response = Response(command="view", success=True, message={"path": output_file_path})
    if (ctx.info_name == "view") or state.verbose:
        if state.json_out is True:
            print(response.model_dump_json())
        else:
            pprint(response.message)


if __name__ == "__main__":
    cli()
