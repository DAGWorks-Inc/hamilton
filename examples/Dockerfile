FROM python:3.9

RUN apt update -y &&\
    apt install git openjdk-11-jdk -y &&\
    git clone https://github.com/bovem/hamilton.git --branch=examples-dockerfile &&\
    cd hamilton/examples &&\
    bash make_python_virtualenv.sh

ENTRYPOINT ["/bin/bash"]
