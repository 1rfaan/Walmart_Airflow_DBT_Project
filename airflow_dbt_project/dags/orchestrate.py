import os
import time

import pendulum
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

@dag(
        dag_id="orchestrate",
        schedule ="0 11 * * *",
        catchup=False,
        start_date=pendulum.datetime(year=2026, month=6, day=18, tz="America/Halifax")
        
)
def orchestrate():
    
    @task
    def ingest_cdc():
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")
        job_id = os.getenv("DATABRICKS_JOB_ID")

        if not host or not token or not job_id:
            raise ValueError(
                "DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_JOB_ID "
                "must be set in the Airflow environment"
            )

        ws = WorkspaceClient(
            host=host,
            token=token,
        )

        job_trigger = ws.jobs.run_now(job_id=int(job_id))
                

        while True:
            
            job_run = ws.jobs.get_run(job_trigger.run_id)
            
            print(f"Job run status: {job_run.state.life_cycle_state}, result state: {job_run.state.result_state}")
            
            
            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully!")
                    break
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")

            time.sleep(5)

        return "CDC Ingestion Completed"
    
    
    
    
    
    
    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_project/target && cd /opt/airflow/walmart_project/logs"
    
    @task.bash
    def source_freshness():
        # Manually set the working directory using the 'cd' command before running
        return "cd /opt/airflow/walmart_project && dbt source freshness --profiles-dir . --target dev"
    
    silver_technical = BashOperator(
        task_id='silver_technical',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select silver_t'
    )
    
    silver_technical_tests = BashOperator(
            task_id='silver_technical_tests',
            cwd='/opt/airflow/walmart_project',
            bash_command='dbt test --select silver_t '
    )
    
    silver_business = BashOperator(
            task_id='silver_business',
            cwd='/opt/airflow/walmart_project',
            bash_command='dbt run --select silver_b '
    )
    
    silver_business_tests = BashOperator(
                task_id='silver_business_tests',
                cwd='/opt/airflow/walmart_project',
                bash_command='dbt test --select silver_b '
    )
    
    gold_ephemeral = BashOperator(
            task_id='gold_ephemeral',
            cwd='/opt/airflow/walmart_project',
            bash_command='dbt run --select gold/ephemeral'
    )
    
    gold_dimensions = BashOperator(
            task_id='gold_dimensions',
            cwd='/opt/airflow/walmart_project',
            bash_command='dbt snapshot'
    )
    
    gold_facts = BashOperator(
            task_id='gold_facts',
            cwd='/opt/airflow/walmart_project',
            bash_command='dbt run --select gold/facts'
    )
    
    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_ephemeral >> gold_dimensions >> gold_facts
    
orchestrate_dag = orchestrate()