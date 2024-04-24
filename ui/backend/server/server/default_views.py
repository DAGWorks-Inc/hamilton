from django.http import HttpResponse


def root_index(request) -> HttpResponse:
    """Showing how we can render some HTML from django."""
    return HttpResponse(
        """
    <html>
      <header></header>
      <body>
        <h1><center>Welcome to the backend server!</center></h1>
        <p><center>Did you mean to visit the <a href="http://localhost:8242">frontend</a>? Note this sometimes takes a bit of time to start up.</center> </p>
      </body>
    </html>
    """
    )
