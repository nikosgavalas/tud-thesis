# python for the VM I am running the experiments.
# alias python='docker run -it -v $(pwd):/tmp python'

FROM python:3.11

WORKDIR /tmp

COPY requirements.txt /tmp/
COPY requirements-dev.txt /tmp/
RUN pip install -r requirements-dev.txt

ENTRYPOINT ["python"]
