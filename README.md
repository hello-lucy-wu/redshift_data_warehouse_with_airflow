 ## Redshift Data Warehouse With Airflow
 ### Commands
 - create a running container, use volumes that map the directories on local machine where DAG definitions and plugins are hold, and the locations where Airflow reads them on the container `docker run -d -p 8080:8080 -v /Users/lucy/dev/data-science-nanodegree/airflow-project/dags:/usr/local/airflow/dags -v /Users/lucy/dev/data-science-nanodegree/airflow-project/plugins/:/usr/local/airflow/plugins puckel/docker-airflow webserver`
 - show container info `docker ps`
 - jump into running container’s command line `docker exec -ti <container ID> bash`
 - test individual tests as part of DAG and logs the output to the command line `airflow test <dag name> <task name> 2020-03-08`
 
