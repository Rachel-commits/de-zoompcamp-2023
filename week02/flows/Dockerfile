From prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip instal -r docker_requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows
COPY data /opt/prefect/data