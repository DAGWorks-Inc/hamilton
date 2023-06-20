import logging
from hamilton.function_modifiers import tag


@tag(cache="str")
def lowercased(initial: str) -> str:
    logging.info("lowercased")
    return initial.lower()


@tag(cache="str")
def uppercased(initial: str) -> str:
    logging.info("uppercased")
    return initial.upper()


@tag(cache="json")
def both(lowercased: str, uppercased: str) -> dict:
    logging.info("both")
    return {"lower": lowercased, "upper": uppercased}

def b2(both: dict) -> dict:
    logging.info("b2")
    return both
