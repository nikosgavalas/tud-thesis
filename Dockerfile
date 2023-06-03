FROM python:3.11

WORKDIR /tmp

COPY . .

RUN pip install -r requirements.txt

ENTRYPOINT ["jupyter-lab"]
