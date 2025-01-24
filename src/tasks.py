# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import subprocess

from openrelik_worker_common.file_utils import create_output_file
from openrelik_worker_common.task_utils import create_task_result, get_input_files

from .app import celery

from google.cloud import bigquery

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-bq.tasks.bq"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "bigquery worker",
    "description": "loads csv data to bigquery",
    # Configuration that will be rendered as a web for in the UI, and any data entered
    # by the user will be available to the task function when executing (task_config).
    "task_config": [
        {
            "name": "project_id",
            "label": "Project ID",
            "description": "Google Cloud Project ID",
            "type": "text",
            "required": True,
        },
        {
            "name": "dataset_id",
            "label": "Dataset ID",
            "description": "BigQuery Dataset ID",
            "type": "text",
            "required": True,
        },
        {
            "name": "table_id",
            "label": "Table ID",
            "description": "BigQuery Table ID",
            "type": "text",
            "required": False,
        },
    ],
}


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def command(
    self,
    pipe_result: str = None,
    input_files: list = None,
    output_path: str = None,
    workflow_id: str = None,
    task_config: dict = None,
) -> str:
    """Load csv files to bigquery.

    Args:
        pipe_result: Base64-encoded result from the previous Celery task, if any.
        input_files: List of input file dictionaries (unused if pipe_result exists).
        output_path: Path to the output directory.
        workflow_id: ID of the workflow.
        task_config: User configuration for the task.

    Returns:
        Base64-encoded dictionary containing task results.
    """
    input_files = get_input_files(pipe_result, input_files or [])
    output_files = []


    if task_config and task_config.get("project_id") and task_config.get("dataset_id"):
        # Construct a BigQuery client object.
        client = bigquery.Client(project=task_config.get("project_id"))
         
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
        )
        dataset = task_config.get("project_id")+"."+task_config.get("dataset_id")
        for input_file in input_files:
            output_file = create_output_file(
                output_path,
                display_name=input_file.get("display_name"),
                extension="out",
            )
            table = dataset+"."+input_file.get("uuid")
            if task_config.get("table_id"):
                table = dataset+"."+task_config.get("table_id")
                
            with open(input_file.get("path"), "rb") as source_file:
                job = client.load_table_from_file(source_file, table, job_config=job_config)
                
            job.result()  # Waits for the job to complete.
                
            bqtable = client.get_table(table)  # Make an API request.
                
            with open(output_file.path, "w") as fh:
                fh.write("Loaded {} rows and {} columns to {}".format(bqtable.num_rows, len(bqtable.schema), table)
            )
            output_files.append(output_file.to_dict())
    else:
        raise RuntimeError("project_id and dataset_id are required")

    if not output_files:
        raise RuntimeError("No output files available")

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        meta={},
    )
