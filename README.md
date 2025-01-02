# Dagster-datapipeline
Data pipeline created in Dagster to schedule data to fetch and store the fetched data into a target database, in this case, PostgresSQL.

Data sources:
1. [Link to Yellow Taxi trips dataset](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet) This dataset is saved with daily and hourly partitions in Postgres DB where the output is being stored.
2. [Link to NOAA hourly data](https://www.ncei.noaa.gov/data/global-hourly/access/)
Steps to use this:

1. Clone this repository
2. Then cd into the folder `dagster-datapipeline` and run `docker-compose up --build -d` or `docker compose up --build -d` if you're using a Mac.
3. Head over to [localhost:3000](http://localhost:3000/locations/pipelines.pipeline1/asset-groups/default) (the same location where Dagster is supposed to run in your local machine).
4. Click `Materialize all` and follow this screenshot to set the upper bound date. ![upper-bound-date](https://github.com/user-attachments/assets/e4c09205-0d32-447c-8a26-f927d707cb44)
5. To check the data, once the pipeline has successfully run execute the following:

