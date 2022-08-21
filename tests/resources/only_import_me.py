"""Module for functions that we import in other test scenarios but never directly in the test case itself."""


def this_is_not_something_we_should_import():
    print("Why am I being printed?")
