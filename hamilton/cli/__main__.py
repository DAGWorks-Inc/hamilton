import dataclasses
import json
import logging
import typing_extensions
from pathlib import Path
from pprint import pprint
from typing import Annotated, Any, Optional

import typer

from hamilton import driver
from hamilton.cli import commands

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Response:
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
    logger.debug(f"verbose set to {verbose}")
    logger.debug(f"json_out set to {json_out}")


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
        logger.debug("calling commands.build()")
        state.dr = commands.build(modules=modules, config=config)
    except Exception as e:
        response = Response(
            command="build", success=False, message={"error": str(type(e)), "details": str(e)}
        )
        logger.error(f"`hamilton build` failed: {dataclasses.asdict(response)}")
        print(json.dumps(dataclasses.asdict(response)))
        raise typer.Exit(code=1)

    response = Response(
        command="build", success=True, message={"modules": [p.stem for p in modules]}
    )

    logger.debug(f"`hamilton build` succeeded: {dataclasses.asdict(response)}")
    if (ctx.info_name == "build") or state.verbose:
        if state.json_out is True:
            print(json.dumps(dataclasses.asdict(response)))
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
        logger.debug("calling commands.diff()")
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
        logger.error(f"`hamilton diff` failed: {dataclasses.asdict(response)}")
        print(json.dumps(dataclasses.asdict(response)))
        raise typer.Exit(code=1)

    response = Response(
        command="diff",
        success=True,
        message=diff,
    )

    logger.debug(f"`hamilton diff` succeeded: {dataclasses.asdict(response)}")
    if (ctx.info_name == "diff") or state.verbose:
        if state.json_out is True:
            print(json.dumps(dataclasses.asdict(response)))
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
        logger.debug("calling commands.version()")
        dataflow_version = commands.version(state.dr)
    except Exception as e:
        response = Response(
            command="version", success=False, message={"error": str(type(e)), "details": str(e)}
        )
        logger.error(f"`hamilton version` failed: {dataclasses.asdict(response)}")
        print(json.dumps(dataclasses.asdict(response)))
        raise typer.Exit(code=1)

    response = Response(
        command="version",
        success=True,
        message=dataflow_version,
    )
    
    logger.debug(f"`hamilton version` succeeded: {dataclasses.asdict(response)}")
    if (ctx.info_name == "version") or state.verbose:
        if state.json_out is True:
            json_str = json.dumps(dataclasses.asdict(response))
            print(json_str)
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
        logger.debug("calling commands.view()")
        commands.view(dr=state.dr, output_file_path=output_file_path)
    except Exception as e:
        response = Response(
            command="view", success=False, message={"error": str(type(e)), "details": str(e)}
        )
        logger.error(f"`hamilton view` failed: {dataclasses.asdict(response)}")
        print(json.dumps(dataclasses.asdict(response)))
        raise typer.Exit(code=1)

    response = Response(command="view", success=True, message={"path": str(output_file_path)})
    
    logger.debug(f"`hamilton view` succeeded: {dataclasses.asdict(response)}")
    if (ctx.info_name == "view") or state.verbose:
        if state.json_out is True:
            print(json.dumps(dataclasses.asdict(response)))
        else:
            pprint(response.message)


if __name__ == "__main__":
    cli()
