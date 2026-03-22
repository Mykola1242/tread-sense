FROM python:latest

WORKDIR /usr/agent

COPY requirements.txt . 

RUN pip install -r requirements.txt
# copy the content of the local src directory to the working directory
COPY src/ .

# command to run on container start
CMD ["python", "main.py"]