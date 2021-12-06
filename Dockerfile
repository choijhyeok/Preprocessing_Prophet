FROM python:3.7.5-slim


#RUN apt update
RUN pip3 install --upgrade pip
RUN pip3 install --no-cache numpy boto3 botocore pandas joblib flask 

WORKDIR /usr/src/app


COPY bootstrap.sh ./
COPY preprocessing_ ./preprocessing_

RUN chmod 755 /usr/src/app/bootstrap.sh
RUN chmod +x /usr/src/app/bootstrap.sh


RUN chmod 755 /usr/src/app/preprocessing_
RUN chmod +x /usr/src/app/preprocessing_


# Start app
EXPOSE 50020
ENTRYPOINT ["/usr/src/app/bootstrap.sh"]
