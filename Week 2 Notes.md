# Week 2 Workflow Orchestration

## Data Lake
#### Data Lake 
- Ingest data as quickly as possible to make the data available.
- Connects data with analytics abd machine learning tools.
- Stores huge amounts of data
- Stream processing and real time analytics
- No versioning
- No metadat associated
- Providers ( GCP - cloud STorage, AWS - S3, Axure - Azue Blob)

#### Data Warehouse
- Structured
- Realtional
- Smaller
#### ETL vs ELT
**ETL** - Data warehouse solution- schema on write)
**ETL** - Data lake solution - scehma on read)

[video](https://www.youtube.com/watch?v=W3Zm6rjOq70&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

## Workflow Orchestration
### 2.2.1 Introduction to Workflow Orchestration
Consists of 
- Workflow -  Task
- Workflow Configuration -  Order of execution
- cond deactivateWorkflow Schedule -  privacy restarts or retries.

Can consist of remote executon, scheduling, retries, caching, integration with external systems (API's databases) Ad-hoc runs, Parameterization, alerting

### Introduction to Prefect

Here we have a basic python flow that ingest data into the postgres database `02_workflow_orchestration/flows/01_start/ingest_data.py`

First we created our vitual environment in the terminal
`conda create -n zommpcamp python=3.9`
`conda activate zoomcamp`

We then had to install our requirements.txt file with `pip install -r requirements.txt`
```
pandas==1.5.2
prefect==2.7.7
prefect-sqlalchemy==0.2.2
prefect-gcp[cloud_storage]==0.2.4
protobuf==4.21.11
pyarrow==10.0.1
pandas-gbq==0.18.1
psycopg2-binary==2.9.5
sqlalchemy==1.4.46
```

We can check the installation with  `prefect version`

#### Transforming the script into a python file
We need a few imports to be able to use the @flow and @task decorator
```
from prefect import flow, task
```
Flows can contain tasks, they are not required for flows but they can receive metadata about upstream flows. We cam also add log_prints and retries. e.g.
`@task(log_prints=True, retries=3`


`prefect orion start`  - This starts the prefect server
`prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api`
Now to run the file `python ./flows/01_start/ingest_data.py`

You can then open up the UI http://127.0.0.1:4200/ and view the flows

#### Blocks
You can then re use the blocks e.g. in the case of credential blocks.
**Example - sql alchemny**
Import the connector in the python file 
```
from prefect_sqlalchemy import SqlAlchemyConnector
```
And then in your function you would load the block as follows
```
connection_block = SqlAlchemyConnector.load("postgres-connector") <name of connector>
with connection_block.get_connection(begin=False) as engine:
```
Within the prefect UI you can then configure the block with the database user name and pasword etc.
an

## ETL with GCP and Prefect
files:
`02_workflow_orchestration/flows/02_gcp/etl_web_to_gcs.py`
`02_workflow_orchestration/flows/02_gcp/etl_gcs_to_bq.py`

### Uploading to GCS
We want to create a new bucket within GCP to store the data - mine is called  `prefect-zoomcamp-23`
Now within the prefect UI we can set up a block. W can register the blocks in the terminal with `prefect block register -m prefect_gcp`
Then we can go to the UI to configure the blocks. I have mine named `zoom-gcs`  here you will also need to enter the name of the gcs bucket. Have also set up a credentials blocks called `zoom-gcp-creds` In this block we will need to get create or use the existing service account. We have given it the following permissions BiqQuery Admin and Storage Admin. The create a json  key. Then you can copy the credentials to the block.

Now in our python flow we  can use the following code 
```
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path=path, to_path=path)
```

Troubleshooting - Due to a bug with windows I needed to add to the upload function
 ```
 path = Path(path).as_posix() 
 ```
### From GCS to Big Query

We can download from gcs to BQ using this function - this assumes there is a data foler in the directory we are running the flow from.
```
def extract_from_gcs(colour: str, year: int, month: int)-> Path:
    """Download trip data from gcs"""
    gcs_path = f"{colour}/{colour}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path = gcs_path, local_path = f"./data/")
    return Path( f"./data/{gcs_path}")
```

Now for the upload we will want a function like this 
```
@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table="dezoomcamp.trips",
        project_id="zoomcamp-376919",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )
```

We can upload the data from GCS to BQ by running our prefect flow `python .\flows\02_gcp\etl_gcs_to_bq.py`


## Parameterizing Workflows
`02_workflow_orchestration/flows/03_deployments/parametrised_flow.py`
Having some code like this  will loop through the months and run the flow for each month. Here we have hard codes months 1,2 and 3 in the main function which will overwrite the 1,2 default.
```
@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2020, colour: str = "green"
):
    for month in months:
        etl_web_to_gcs(colour,year, month)

if __name__ == "__main__":
    colour = "green"
    months = [1, 2, 3]
    year = 2020
    etl_parent_flow(months, year, colour)
```

Troubleshooting - I had to remove the cash_key_fn and cache_expiration from the fetch function as it was causing errors when .

We can run the flow `python .\flows\03_deployments\parameterised_flow.py`

Up until now we have just been running the flows mnaully  but part of orchestartion id the ability to automae. In oreder to do this we will need to creae deployments.We can do this by creating a deployment definition. For more information https://docs.prefect.io/concepts/deployments/

You could have mutiple deployments for a single flow e.g. one deployment to run with yellow or one with green.

### Deploying through the CLI
In terminal we can run the deployment command
`prefect deployment build <filename>:<entrypoint to the flow> -n <"Name of the deployment">`
`prefect deployment build  .\flows\03_deployments\parameterised_flow.py:etl_parent_flow -n "New Parametrised ETL"`

If it has been succesful we will see the following and a yaml file will be created in the directory the coommand was run from
```
Found flow 'etl-parent-flow'
Deployment YAML created at 
'C:\Users\rache\Documents\coding\de_zoomcamp_2023\02_workflow_orchestration\etl_parent_flow-deployment.yaml'.
Deployment storage None does not have upload capabilities; no files uploaded.  Pass --skip-upload to suppress this warning.

```

We can then edit the yaml with information regarding the deployment e.g. we could set the default parameters 
```
parameters: {"months":[1, 2], "year":2020, "colour":yellow"}
```
Now we want to apply the metadat to the deployment using the following command 
`prefect deployment apply <name of the yaml file>`
 `prefect deployment apply etl_parent_flow-deployment.yaml`

 If successful we will see the following and we will be able to view the deployment in the UI
 ```
 Successfully loaded 'New Parametrised ETL'
Deployment 'etl-parent-flow/New Parametrised ETL' successfully created with id '5fc18986-bd90-411b-a325-9746dbfda949'.
View Deployment in UI: http://127.0.0.1:4200/deployments/deployment/5fc18986-bd90-411b-a325-9746dbfda949

To execute flow runs from this deployment, start an agent that pulls work from the 'default' work queue:
$ prefect agent start -q 'default'
 ```

### Work Queues and Agents

 From the UI we could trigger a quick run from UI  However if we look at the flow run we will see its in a scheduled state as we do not have an agent to pick up the run.

An agent is a lightweight Python process that lives in our execution environment (local for us)
Copy this command (it is available on the Work Queues tab in Orion): `prefect agent start  --work-queue "default"`
This will then complete the flow run that we had just triggered in the deployment.

Within deployments in the UI you can add a schedule
interval, cron

#### cron syntax
* * * * *
minute  hour day month day_of_week

RRule is recurring rule - you can't do that from the UI at the moment.

You can add the schedule on the depoy buily command by adding the -a tag it will apply as well
`prefect deployment build  .\flows\03_deployments\parameterised_flow.py:etl_parent_flow -n "New Parametrised ETL2" --cron "0 0 * * *" -a`

We can use the set-schedule command if we have already built the deployment.

For help
`prefect deployment  --help`

We can add the scedule after creating the deployment 


## Adding Deployment to Docker
Here we are going to store our code into a docker image
We need to make a Dockerfile (no extension in our top level directory)

We made a docker requirements docker_requirements.txt file which contained the following
```
pandas==1.5.2
prefect-gcp[cloud_storage]==0.2.4
protobuf==4.21.11
pyarrow==10.0.1
pandas-gbq==0.18.1
```

```
From prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker_requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows
COPY data /opt/prefect/data
```

We now want to build the docker image

`docker image build -t <dockerhub username>/<tagname>`
`docker image build -t rachzoom/prefect:zoom .`

Make sure you are logged in then you can push the image
`docker image push rachzoom/prefect:zoom`

Now anyone can access this from docker
Now in orefect we will create a docker block  here you can specify the image name you have used already i.e. `rachzoom/prefect:zoom` For pull policy we selected `ALWAYS`
Autonremove select rtue and then create. Now it will have the code ready to use in the deployment.

Previously we reated a deployment int he CLI now we will make a deployment using python code in the file `02_workflow_orchestration/flows/03_deployments/docker_deploy.py`

```
from prefect.deployments import Deployment 
from prefect.infrastructure.docker import DockerContainer
from parameterised_flow import etl_parent_flow 

docker_block = DockerContainer.load("zoom")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name='docker_flow',
    infrastructure=docker_block)

if __name__ == "__main__":
    docker_dep.apply()
```
Now in terminal we can run `python ./flows/03_deployments/docker_deploy.py`  This will create the deployment which we will now be able to view in the ui.

Now if we look at the profiles `prefect profiles ls` we will see that we are using the default profile.
We can run `prefect config set  "PREFECT_API_URL="http://127.0.0.1:4200/api"`  so the docker isinteracting with the correct profile

Can set the agent and then run the flow
`prefect agent start  -q default`
`prefect deployment run etl-parent-flow/docker_flow2 -p "months=[6,7]"`
We will see
```
Creating flow run for deployment 'etl-parent-flow/docker_flow2'...
Created flow run 'impartial-gorilla'.
└── UUID: 40f0cc64-4095-40b9-949b-6e84dcd9ffd7
└── Parameters: {'months': [6, 7]}
└── Scheduled start time: 2023-02-12 14:36:30 GMT (now)
└── URL: http://127.0.0.1:4200/flow-runs/flow-run/40f0cc64-4095-40b9-949b-6e84dcd9ffd7
```

## Prefect Cloud and Additional Resources
