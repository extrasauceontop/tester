FROM safegraph/apify-python3:latest

COPY . ./

USER root

RUN pip3 install -r requirements.txt


RUN pip3.8 install certifi && ln -s /usr/local/lib/python3.8/site-packages/certifi/cacert.pem /usr/local/ssl/cert.pem

CMD npm start