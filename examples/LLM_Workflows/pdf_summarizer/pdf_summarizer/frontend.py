import base64
import streamlit as st


@st.cache_data
def pdf_to_base64(uploaded_file):
    """Encode the file's bytes into a b64 string"""
    return base64.b64encode(uploaded_file.getvalue()).decode("utf-8")


def container_pdf_viewer(uploaded_file) -> None:
    """Display the PDF as an embedded b64 string in a markdown component"""
    base64_pdf = pdf_to_base64(uploaded_file)
    pdf_display = f'<embed src="data:application/pdf;base64,{base64_pdf}" width=600 height=800 type="application/pdf">'
    st.markdown(pdf_display, unsafe_allow_html=True)


def summarize_callback(file):
    """`Summarize` button callback; handle input validation and logic"""
    if file:
        # response = requests.TO_FASTAPI
        # st.session_state["summary"] = response.content
        raise NotImplementedError
        

def app() -> None:
    st.set_page_config(
        page_title="PDF-Summarizer",
        page_icon="📝",
        layout="centered",
        menu_items={"Get help": None, "Report a bug": None},
    )
    st.title("PDF-Summarizer 📝")

    col1, col2 = st.columns(2)

    with col1:
        uploaded_file = st.file_uploader("Upload PDF", type=["pdf"], label_visibility="hidden")
        st.button("Summarize", on_click=summarize_callback, args=(uploaded_file,), type="primary", use_container_width=True)

        if summary := st.session_state.get("summary", None):
            with st.expander("Summary", expanded=False):
                st.write(summary)

            st.download_button("Download", data=summary, use_container_width=True)

    with col2:
        if uploaded_file:
            container_pdf_viewer(uploaded_file)


if __name__ == "__main__":
    app()
