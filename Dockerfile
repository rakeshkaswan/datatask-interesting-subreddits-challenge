FROM python:3.7

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

COPY . /app
#WORKDIR /app

# Install python packages
#RUN pip install --no-cache-dir -r /app/requirements.txt
RUN pip install -r /app/requirements.txt

RUN mkdir -p /app/data

WORKDIR /app/data
RUN wget https://latana-data-eng-challenge.s3.eu-central-1.amazonaws.com/allposts.csv

WORKDIR /app


CMD ["python", "./run.py"]
