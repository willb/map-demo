FROM radanalyticsio/openshift-spark

USER root

ADD requirements.txt wheel-requirements.txt run.sh /opt/map-demo/

WORKDIR /opt/map-demo

RUN yum install -y python-pip \
 && pip install -r requirements.txt \
 && pip wheel -r wheel-requirements.txt -w . \
 && mv pymongo*.whl pymongo.zip

ADD . /opt/map-demo/

EXPOSE 8080

LABEL io.k8s.description="Spark map demo." \
      io.k8s.display-name="Spark map demo." \
      io.openshift.expose-services="8080:http"

USER 185

CMD ./run.sh
