FROM python:3
RUN apt-get update && apt-get -y install unixodbc-dev mdbtools
ADD ./code /code
ADD run.sh /
RUN chmod +x run.sh
RUN cd code &&  pip install -r requirements.txt
CMD [ "bash", "./run.sh" ]