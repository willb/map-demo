FROM radanalyticsio/openshift-spark

USER root

ADD . /opt/map-demo

WORKDIR /opt/map-demo

RUN yum install -y python-pip \
 && pip install -r requirements.txt \
 && pip wheel -r wheel-requirements.txt -w . \
 && mv pymongo*.whl pymongo.zip

USER 185

CMD ./run.sh
