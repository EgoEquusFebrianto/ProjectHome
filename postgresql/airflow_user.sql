create user airflow with password 'airflow_user_password';
grant all privileges on database retail_transaction to airflow;

grant all on schema airflow_api_server to airflow;
grant all privileges on all tables in schema airflow_api_server to airflow;
grant all privileges on all sequences in schema airflow_api_server to airflow;

GRANT USAGE, CREATE ON SCHEMA airflow_retail_dataset TO airflow;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA airflow_retail_dataset TO airflow;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA airflow_retail_dataset TO airflow;

alter schema airflow_api_server owner to airflow;
ALTER ROLE airflow SET search_path = airflow_api_server;	