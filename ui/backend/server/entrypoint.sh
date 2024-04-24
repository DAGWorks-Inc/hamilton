set -e

python manage.py migrate  # Apply database migrations
python manage.py runserver 0.0.0.0:8241  # Start the server
