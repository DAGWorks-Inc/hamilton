from django.http import HttpResponse


def root_index(request) -> HttpResponse:
    """Showing how we can render some HTML from django."""
    return HttpResponse(
        """
    <html>
      <header></header>
      <body>
        <h1><center>Welcome to the backend server!</center></h1>
      </body>
    </html>
    """
    )
