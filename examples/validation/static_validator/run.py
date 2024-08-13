from typing import Optional

from hamilton.graph_types import HamiltonNode
from hamilton.lifecycle import api


class MyTagValidator(api.StaticValidator):
    def run_to_validate_node(
        self, *, node: HamiltonNode, **future_kwargs
    ) -> tuple[bool, Optional[str]]:
        if node.tags.get("node_type", "") == "output":
            table_name = node.tags.get("table_name")
            if not table_name:  # None or empty
                error_msg = f"Node {node.tags['module']}.{node.name} is an output node, but does not have a table_name tag."
                return False, error_msg
        return True, None


if __name__ == "__main__":
    import good_module

    from hamilton import driver

    tag_validator = MyTagValidator()
    dr = driver.Builder().with_modules(good_module).with_adapters(tag_validator).build()
    print(dr.execute([good_module.foo]))

    import bad_module

    # this should error
    dr = driver.Builder().with_modules(bad_module).with_adapters(tag_validator).build()
