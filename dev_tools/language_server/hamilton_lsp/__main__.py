from hamilton_lsp.server import HamiltonLanguageServer, regiser_server_features


# TODO use argparse to allow
#   - io, tcp, websocket modes
#   - select host and port
def main():
    language_server = HamiltonLanguageServer()
    language_server = regiser_server_features(language_server)

    language_server.start_io()
    # tcp is good for debugging
    # server.start_tcp("127.0.0.1", 8087)


if __name__ == "__main__":
    import logging
    import warnings

    logger = logging.getLogger("pygls")
    logger.addHandler(logging.StreamHandler())

    # silence UserWarning: 'PYARROW_IGNORE_TIMEZONE'
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)

        main()
