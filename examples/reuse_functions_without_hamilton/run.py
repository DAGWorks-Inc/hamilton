import external_decorators
import my_functions
from decorator_utilities import remove_hamilton, use_external_decorators

from hamilton import driver

try:
    import my_functions__undecorated
except ModuleNotFoundError:
    pass

# You will need to run the script twice if `my_functions__undecorated.py`
# is not in the directory
if __name__ == "__main__":
    # will generate `my_functions__undecorated.py`
    undecorated_module = remove_hamilton(my_functions, save_module=True)

    try:
        decorated_module = use_external_decorators(
            functions_module=my_functions__undecorated,
            decorators_module=external_decorators,
            save_module=True,
        )
    except:  # noqa: E722
        pass

    # decorated_module = use_external_decorators(
    #     source_module="./my_functions__undecorated.py",
    #     decorators_module="./external_decorators.py",
    #     save_module=False,
    # )

    dr = driver.Driver({}, decorated_module)
    dr.display_all_functions("hamilton.dot")
