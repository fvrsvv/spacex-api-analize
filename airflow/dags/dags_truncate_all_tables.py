"""DAG truncate all tables"""

import utils as u
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from sqlalchemy.orm import Session
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def truncate_table(postgres_conn_id="server_publicist"):
    try:
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        cursor.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public'
        """)
        tables = cursor.fetchall()
        if not tables:
            logger.info("Not found tables for truncate")
            return 0
        cursor.execute("SET session_replication_role = 'replica';")

        for table in tables:
            table_name = table[0]
            try:
                cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
                logger.info(f"Table {table_name} truncate successfully")
            except Exception as e:
                logger.error(f"Error truncate with table {table_name}: {str(e)}")
                connection.rollback()
                raise
        cursor.execute("SET session_replication_role = 'origin';")

        connection.commit()
        logger.info(f"All tables successfully truncate. Proccesed tables: {len(tables)}")
        return len(tables)

    except Exception as e:
        logger.error(f"Error truncate tables: {str(e)}")
        if connection:
            connection.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

dag = DAG(
    dag_id="dags_truncate_all_tables",
    start_date=days_ago(5),
    schedule_interval=None,
    catchup=False,
    tags=["truncate", "cleanup"],
)

dags_truncate_all_tables = PythonOperator(
    task_id="dags_truncate_all_tables", 
    python_callable=truncate_table,
    op_kwargs={
        "postgres_conn_id": "server_publicist",
    },
    dag=dag,
)

dags_truncate_all_tables