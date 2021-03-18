# Examples

Run Airflow containing the examples in the `dags` folder, by executing
```
docker-compose up
```

:warning ** The docker-compose.yml does mount your personal ~/.config folder. This is to make the examples to easily work with Google Storage buckets. If you are uncomfortable with this, remove the mount from docker-compose.yml and add Connection settings yourself.**

## DAGS
### gcs_sensor.py
This DAG creates a sensor that listens for arrivals of files with the pattern `file_20210101`, where the last 8 digits represents the date that will be executed.

Add the variable `gcs_sensor_bucket` and reference a bucket that you have access to. Copy a file, with that pattern, and watch the sensor DAG trigger the Hello world DAG.