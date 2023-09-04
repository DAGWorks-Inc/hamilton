import requests
from streamlit.runtime.uploaded_file_manager import UploadedFile

# the SERVER_URL matches the name of the service in the docker compose network bridge
SERVER_URL = "http://fastapi_server:8082"


def get_fastapi_status(server_url: str = SERVER_URL):
    """Access FastAPI /docs endpoint to check if server is running"""
    try:
        response = requests.get(f"{server_url}/docs")
        if response.status_code == 200:
            return True
    except requests.exceptions.RequestException:
        return False


def post_store_arxiv(arxiv_ids: list[str], server_url: str = SERVER_URL):
    """Send POST request to FastAPI /store_arxiv endpoint"""
    payload = dict(arxiv_ids=arxiv_ids)
    response = requests.post(f"{SERVER_URL}/store_arxiv", data=payload)
    return response


def post_store_pdfs(pdf_files: list[UploadedFile], server_url: str = SERVER_URL):
    """Send POST request to FastAPI /store_pdfs endpoint"""
    files = [("pdf_files", f) for f in pdf_files]
    response = requests.post(f"{SERVER_URL}/store_pdfs", files=files)
    return response


def get_rag_summary(
    rag_query: str,
    hybrid_search_alpha: float,
    retrieve_top_k: int,
    server_url: str = SERVER_URL,
):
    """Send GET request to FastAPI /rag_summary endpoint"""
    payload = dict(
        rag_query=rag_query, hybrid_search_alpha=hybrid_search_alpha, retrieve_top_k=retrieve_top_k
    )
    response = requests.get(f"{SERVER_URL}/rag_summary", data=payload)
    return response


def get_all_documents_file_name():
    """Send GET request to FastAPI /documents endpoint"""
    response = requests.get(f"{SERVER_URL}/documents")
    return response
