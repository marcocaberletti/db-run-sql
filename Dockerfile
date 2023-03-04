FROM python:3.11.2-slim

RUN apt-get update && \
    apt-get -y install gcc default-libmysqlclient-dev && \
    apt-get clean all

ADD requirements.txt .
RUN pip3 install -r requirements.txt
ADD *.py .

CMD [ "./db-run-sql.py" ]
