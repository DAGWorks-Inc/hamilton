from lsprotocol.types import (
    TEXT_DOCUMENT_DOCUMENT_SYMBOL,
    DocumentSymbolParams,
    TextDocumentIdentifier,
)


# NOTE Placeholder for a test
def test_something(client_server):
    client, _ = client_server
    response = client.lsp.send_request(
        TEXT_DOCUMENT_DOCUMENT_SYMBOL,
        DocumentSymbolParams(
            text_document=TextDocumentIdentifier(uri="file://resources/dataflow.py")
        ),
    ).result()

    assert response
