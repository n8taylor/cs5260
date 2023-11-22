FROM python:3.8-slim
COPY consumer.py consumer.py
RUN pip install boto3==1.28.62
CMD ["python", "consumer.py", "-qt", "sqs", "-qn", "cs5260-requests", "-st", "s3", "-sn", "cs-5260-wizard-web"]
