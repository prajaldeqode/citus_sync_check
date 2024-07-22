FROM python:3.9

WORKDIR /CitusSyncCheck

ENV PYTHONPATH="/CitusSyncCheck/"
ENV PIP_CACHE_DIR=/tmp/pip_cache
# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY ./src .

RUN pip install prometheus_client==0.20.0
RUN pip install psycopg2-binary==2.9.9
RUN pip install requests==2.32.3
RUN pip install schedule==1.2.2
RUN pip install python-dotenv==1.0.1

CMD ["python3", "/CitusSyncCheck/main.py"]
