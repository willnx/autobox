FROM ubuntu:18.04
RUN apt-get update && apt-get upgrade -y && apt-get install -y ca-certificates

# Setup EMC Root CA
COPY emc_root_ca.cer /
RUN mkdir -p /usr/share/ca-certificates/extra && \
    mv /emc_root_ca.cer /usr/share/ca-certificates/extra && \
    update-ca-certificates

# Setup vsphere-influxdb-go
COPY *.deb /
RUN dpkg -i /*.deb && apt-get install -y -f && rm /*.deb
COPY vsphere-influxdb-go.json /etc
COPY run.sh /
CMD /run.sh
