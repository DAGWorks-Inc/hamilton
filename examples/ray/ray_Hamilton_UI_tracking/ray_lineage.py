import time


def node_5s() -> float:
    start = time.time()
    time.sleep(5)
    return time.time() - start


def add_1_to_previous(node_5s: float) -> float:
    start = time.time()
    time.sleep(1)
    return node_5s + (time.time() - start)


def node_1s_error(node_5s: float) -> float:
    time.sleep(1)
    raise ValueError("Does not break telemetry if executed through ray")
