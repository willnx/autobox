#!/bin/bash
#
# Collect metrics from VMware and push them into an InfluxDB server

update_config () {
  # Read env-vars and update the JSON config
  CONFIG_FILE=/etc/vsphere-influxdb-go.json
  sed -i -e "s/VSPHERE_USERNAME/${VSPHERE_USERNAME}/g" ${CONFIG_FILE}
  sed -i -e "s/VSPHERE_PASSWD/${VSPHERE_PASSWD}/g" ${CONFIG_FILE}
  sed -i -e "s/VSPHERE_SERVER/https:\/\/${VSPHERE_SERVER}/g" ${CONFIG_FILE}

  sed -i -e "s/INFLUXDB_SERVER/https:\/\/${INFLUXDB_SERVER}/g" ${CONFIG_FILE}
  sed -i -e "s/INFLUXDB_USER/${INFLUXDB_USER}/g" ${CONFIG_FILE}
  sed -i -e "s/INFLUXDB_PASSWD/${INFLUXDB_PASSWD}/g" ${CONFIG_FILE}
  sed -i -e "s/INFLUXDB_TARGET_DB/${INFLUXDB_TARGET_DB}/g" ${CONFIG_FILE}
}

main () {
  update_config
  LOOP_INTERVAL=60 # seconds
  while true; do
    START_TIME=$(date +'%s')
    /usr/local/bin/vsphere-influxdb-go -test
    END_TIME=$(date +'%s')
    ELAPSED_TIME=$((END_TIME - START_TIME))
    WAIT_TIME=$((LOOP_INTERVAL - ELAPSED_TIME))
    sleep ${WAIT_TIME}
  done
}
main
