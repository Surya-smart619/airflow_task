# airflow_task

Refer: <https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html>

=================Download airflow docker-compose file===============================

curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.4.3/docker-compose.yaml'

=================Create volumes path====================================

mkdir -p ./dags ./logs ./plugins
mkdir -p ./dags ./logs ./plugins

===============start airflow=================
docker-compose up airflow-init
docker-compose up

============cleaning up===========
docker-compose down --volumes --rmi all

====================================
