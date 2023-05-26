# python for the VM I was using for the experiments.
# outdated now.

# alias python='docker run -it -v $(pwd):/tmp python'

FROM python:3.11

WORKDIR /tmp

COPY . .

RUN pip install '.[benchmarks]'

ENTRYPOINT ["python"]
