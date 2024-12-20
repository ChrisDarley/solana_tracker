import os
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from dags.chrisdarley.chrisdarley_append_tokens_to_src import load_tokens
from dags.chrisdarley.chrisdarley_get_token_prices import run_pipeline

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig  # type: ignore

from airflow.operators.empty import EmptyOperator  # type: ignore
from dotenv import load_dotenv  # type: ignore

dbt_env_path = os.path.join(os.environ["AIRFLOW_HOME"], "dbt_project", "dbt.env")
load_dotenv(dbt_env_path)

airflow_home = os.getenv(
    "AIRFLOW_HOME"
)  # ASK ABOUT THIS - where can i find a reference to this?
PATH_TO_DBT_PROJECT = f"{airflow_home}/dbt_project"
PATH_TO_DBT_PROFILES = f"{airflow_home}/dbt_project/profiles.yml"

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath=PATH_TO_DBT_PROFILES,
)

default_args = {
    "owner": "Chris Darley",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


@dag(
    dag_id="ChrisDarley_capstone_dag",
    description="DAG for loading src data and running dbt models",
    start_date=datetime(2024, 12, 6),
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    default_args=default_args,
    tags=["chrisdarley"],
)
def chrisdarley_capstone_dag():
    # Step 1: Python task to update the src table
    @task(task_id="update_src_table")
    def update_src_table(**kwargs):
        logical_date = kwargs["logical_date"]
        formatted_execution_time = logical_date.strftime("%Y-%m-%d %H:%M:%S")
        load_tokens(formatted_execution_time)
        print("SRC table updated.")

    # @task(task_id="build_wallet_balances_src")
    # def build_wallet_balances_src(**kwargs):
    #     logical_date = kwargs["logical_date"]
    #     formatted_execution_time = logical_date.strftime("%Y-%m-%d %H:%M:%S")
    #     get_all_wallet_balances(formatted_execution_time)

    # Step 2: Empty Operator to signify transition
    pre_dbt_workflow = EmptyOperator(task_id="pre_dbt_workflow")

    # Step 3: dbt Task Group for running dbt models
    dbt_task_group = DbtTaskGroup(
        group_id="dbt_staging_and_downstream",
        # line below changed from ...(DBT_PROJECT_PATH)...
        project_config=ProjectConfig(PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        render_config=RenderConfig(select=["+dim_unique_tokens"]),
    )

    # Step 4: Empty Operator to signify post-dbt actions
    post_dbt_workflow = EmptyOperator(task_id="post_dbt_workflow")

    @task(task_id="get_prices")
    def get_prices(**kwargs):
        logical_date = kwargs["logical_date"]
        formatted_execution_time = logical_date.strftime("%Y-%m-%d %H:%M:%S")
        run_pipeline(formatted_execution_time)
        print("token prices added")

    pre_dbt_workflow_2 = EmptyOperator(task_id="pre_dbt_workflow_2")

    dbt_task_group_prices = DbtTaskGroup(
        group_id="constructing_price_tables",
        project_config=ProjectConfig(PATH_TO_DBT_PROJECT),
        profile_config=profile_config,
        render_config=RenderConfig(select=["+price_analytics"]),
    )

    # Step 4: Empty Operator to signify post-dbt actions
    post_dbt_workflow_2 = EmptyOperator(task_id="post_dbt_workflow_2")

    (
        update_src_table()
        # >> build_wallet_balances_src()
        >> pre_dbt_workflow
        >> dbt_task_group
        >> post_dbt_workflow
        >> get_prices()
        >> pre_dbt_workflow_2
        >> dbt_task_group_prices
        >> post_dbt_workflow_2
    )


chrisdarley_capstone_dag()
