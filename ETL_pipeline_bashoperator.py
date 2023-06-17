from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

# /you can create default arguments
default_args = {
    "owner": "MaryiaPyshynskaya",
    "depends_on_past": False,
    # /in case it fails, try 2 times
    "retries": 2,
    # /delay 30 secs
    "retry_delay": timedelta(seconds=10),
}
# Create a DAG
with DAG(
    "BasicETL",  # name of the file
    description="Basic ETL Dag",
    default_args=default_args,
    # /timedelta helps specify if the dag should in days(days=1(start date)), seconds, hours
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 06, 1)
    # end_date=datetime(2023, 06, 2),
) as dag:

    # /create the task using the bashOperator
    taskA = BashOperator(
        task_id="task1",
        #go to the downloads folder and match the pattern of a zip file and writes the file names to the text file
        bash_command="ls /Users/37529/Downloads | grep 'zip' > /Users/37529/My projects/ETL Pipeline Airflow/results.txt",
    )

    taskB = BashOperator(
        task_id="task2",
        #the command reads the contents of the file results.txt and saves the same content into a new file named Newresults.txt
        bash_command="cat /Users/37529/My projects/ETL Pipeline Airflow/results.txt > /Users/37529/My projects/ETL Pipeline Airflow/Newresults.txt",
    )

    taskC = BashOperator(
        task_id="task3",
        #command creates a new directory named "PipelineFolder" in the "/Users/37529/My projects/ETL Pipeline Airflow" directory
        bash_command="mkdir /Users/37529/My projects/ETL Pipeline Airflow/PipelineFolder"
    )

    task1 >> task2 >> task3
