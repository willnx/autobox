FROM willnx/vlab-base

COPY  dist/*.whl /tmp
RUN pip3 install /tmp/*.whl && rm /tmp/*.whl
RUN apk del gcc

WORKDIR /usr/lib/python3.6/site-packages/log_exporter
CMD ["python3", "main.py"]
