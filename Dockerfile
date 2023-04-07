# python for the VM I am running the experiments.
# alias python='docker run -it -v $(pwd):/tmp python'

FROM python:3.11

WORKDIR /tmp

RUN pip install .

ENTRYPOINT ["python"]
