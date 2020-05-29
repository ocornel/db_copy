FROM python:3
ADD ./code /code
ADD run.sh /
RUN chmod +x run.sh
RUN cd code &&  pip install -r requirements.txt 
CMD [ "bash", "./run.sh" ]
