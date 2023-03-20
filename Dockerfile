# Use an official Python runtime as a parent image
FROM python:3.10.9
ENV PYTHONUNBUFFERED 1
COPY ./requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && apt-get -y install wkhtmltopdf
RUN pip install --no-cache-dir -r /requirements.txt
RUN mkdir -p /app
COPY . /app
WORKDIR /app
EXPOSE 5000
CMD ["python", "app.py"]