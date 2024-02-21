import dataclasses
import json
import logging
import sys
import warnings
from pathlib import Path
from pprint import pprint
from typing import Any, List, Optional

if sys.version_info < (3, 9):
    from typing_extensions import Annotated
else:
    from typing import Annotated
   
import typer

# silence UserWarning: 'PYARROW_IGNORE_TIMEZONE' 
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
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


cli = typer.Typer(rich_markup_mode="rich")
state = CliState()


# TODO add `validate` command to call `Driver.validate()`
# TODO add `experiments` for `hamilton.plugins.h_experiments`
# TODO add `dataflows` submenu to manage locally installed dataflows
# TODO add `init` to load project template
# entrypoint for `hamilton` without command
@cli.callback()
def main(
    verbose: Annotated[
        bool, 
        typer.Option(
            help="Output all intermediary commands", rich_help_panel="Output format",
        )
    ] = False,
    json_out: Annotated[
        bool, 
        typer.Option(
            help="Output JSON for programmatic use (e.g., CI)", rich_help_panel="Output format",
        )
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
        List[Path], 
        typer.Argument(
            help="Paths to Hamilton modules",
            exists=True, dir_okay=False, readable=True, resolve_path=True,
        )
    ],
):
    """Build a single Driver with MODULES"""
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


# TODO add option to output diff of nodes and diff of functions
# since the function diff is what's useful for code reviews / debugging 
@cli.command()
def diff(
    ctx: typer.Context,
    modules: Annotated[
        List[Path], 
        typer.Argument(
            help="Paths to Hamilton modules",
            exists=True, dir_okay=False, readable=True, resolve_path=True
        )
    ],
    git_reference: Annotated[
        str,
        typer.Option(
            help="[link=https://git-scm.com/book/en/v2/Git-Internals-Git-References]git reference[/link] to compare to"
        )
    ] = "HEAD",
    view: Annotated[
        bool,
        typer.Option(
            "--view",
            "-v",
            help="Generate a dataflow diff visualization",
        )
    ] = False,
    output_file_path: Annotated[
        Path,
        typer.Option(
            "--output",
            "-o",
            help="File path of visualization",
            exists=False,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ] = "diff.png",
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
            output_file_path=output_file_path,
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
        List[Path],
        typer.Argument(
            help="Paths to Hamilton modules",
            exists=True, dir_okay=False, readable=True, resolve_path=True
        )
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
        List[Path],
        typer.Argument(
            help="Paths to Hamilton modules",
            exists=True, dir_okay=False, readable=True, resolve_path=True
        )
    ],
    output_file_path: Annotated[
        Path,
        typer.Option(
            "--output",
            "-o",
            help="File path of visualization",
            exists=False,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ] = "./dag.png",
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
