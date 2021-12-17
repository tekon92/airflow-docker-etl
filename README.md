# airflow-docker-etl
create your first mini etl using airflow.


this example of mini dwh, where we create the workflow of automation, 
we start get the data from API, store it into staging on mysql then create our own datawarehouse on postgresql.


# step by step
1. Clone this report
2. make sure you have docker compose installed
3. after clone then move into directory of docker-compose.yml
4. execute docker-compose up -d, everything should be up and running
5. then go to dags/sql/ddl.sql, execute that on postgresql (username n password on database.env
6. open airlflow localhost:8080 (username: user , password: bitnami)
7. then run the dags or scheduled
