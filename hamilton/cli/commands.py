from pathlib import Path
from typing import List, Optional

from hamilton import ad_hoc_utils, driver
from hamilton.cli import logic


def build(modules: List[Path], config: Optional[dict] = None):
    config = config if config else {}
    module_objects = [ad_hoc_utils.module_from_source(p.read_text()) for p in modules]
    return (
        driver.Builder()
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_modules(*module_objects)
        .with_config(config)
        .build()
    )


def diff(
    dr: driver.Driver,
    modules: List[Path],
    git_reference: Optional[str] = "HEAD",
    view: bool = False,
    output_file_path: Path = Path("./diff.png"),
    config: Optional[dict] = None,
) -> dict:
    config = config if config else {}

    original_version = logic.hash_hamilton_nodes(dr)

    reference_modules = logic.load_modules_from_git(modules, git_reference)
    reference_dr = (
        driver.Builder()
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_modules(*reference_modules)
        .with_config(config)
        .build()
    )
    reference_version = logic.hash_hamilton_nodes(reference_dr)

    # v1 and v2 mustc match the dr1 and dr2 of `visualize_diff`
    diff = logic.diff_versions(
        mapping_v1=reference_version,
        mapping_v2=original_version,
    )

    if view:
        # v1 and v2 mustc match the dr1 and dr2 of `diff_versions`
        dot = logic.visualize_diff(dr1=reference_dr, dr2=dr, **diff)
        
        # simplified logic from hamilton.graph.display()
        output_format = "png"
        if output_file_path:  # infer format from path
            if output_file_path.suffix != "":
                output_format = output_file_path.suffix.partition(".")[-1]
        
        dot.render(output_file_path.with_suffix(""), format=output_format)

    return diff


def version(dr: driver.Driver) -> dict:
    nodes_hash = logic.hash_hamilton_nodes(dr)
    dataflow_hash = logic.hash_dataflow(nodes_hash)
    return dict(
        nodes_hash=nodes_hash,
        dataflow_hash=dataflow_hash,
    )


def view(dr: driver.Driver, output_file_path: str) -> None:
    dr.display_all_functions(output_file_path)
