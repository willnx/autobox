##############
Log Processing
##############

The vLab server and user gateways send logs to Kafka for processing.

All vLab services that send logs to Kafka are expected to put those logs
in the correct topic. This simplifies the logic in the specific processors, and
enables us to put each process in it's own Linux container.
