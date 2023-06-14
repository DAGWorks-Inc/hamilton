# Airflow setup
1. Write `echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env`
2. Write `docker compose up --build`
3. Connect via `http://localhost:8080`. The default username is `airflow` and the password is `airflow`

# Notes

Currently, airflow doesn't support Python 3.11 and must be installed via pip
