FROM python:3.11

WORKDIR /tmp

COPY requirements.txt .

EXPOSE 8888

RUN pip install -r requirements.txt

CMD ["jupyter-lab", "--allow-root", "--ip=0.0.0.0", "--no-browser"]
