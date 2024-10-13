from airflow.providers.google.cloud.operators.bigquery import (BigQueryCreateExternalTableOperator, 
                                                               BigQueryCreateEmptyTableOperator, 
                                                               BigQueryInsertJobOperator,
                                                               BigQueryDeleteTableOperator)

def create_external_table(event,
                          gcp_project_id,
                          bigquery_dataset,
                          external_table_name,
                          gcp_gcs_bucket,
                          events_path):
    
    task = BigQueryCreateExternalTableOperator(
        task_id = f'{event}_create_external_table',
        table_resource = {
            'tableReference': {
                'projectId': gcp_project_id,
                'datasetId': bigquery_dataset,
                'tableId': f'{external_table_name}',
            },
            'externalDataConfig': {
                'sourceFormat': 'PARQUET',
                'sourceUris': [f'gs://{gcp_gcs_bucket}/{events_path}/*'],
            },
        }
    )

    return task


def create_empty_table(event,
                       gcp_project_id,
                       bigquery_dataset,
                       bigquery_table_name,
                       events_schema):
    
    task = BigQueryCreateEmptyTableOperator(
        task_id = f'{event}_create_empty_table',
        project_id = gcp_project_id,
        dataset_id = bigquery_dataset,
        table_id = bigquery_table_name,
        schema_fields = events_schema,
        time_partitioning = {
            'type': 'HOUR',
            'field': 'ts'
        },
        exists_ok = True
    )

    return task


def insert_job(event,
               insert_query_location,
               bigquery_dataset, 
               gcp_project_id,
               timeout=300000):
    
    task = BigQueryInsertJobOperator(
        task_id = f'{event}_execute_insert_query',
        configuration = {
            'query': {
                'query': insert_query_location,
                'useLegacySql': False
            },
            'timeoutMs': timeout,
            'defaultDataset': {
                'datasetId': bigquery_dataset,
                'projectId': gcp_project_id
            }
        }
    )

    return task


def delete_external_table(event,
                          gcp_project_id,
                          bigquery_dataset,
                          external_table_name):
    
    task = BigQueryDeleteTableOperator(
        task_id = f'{event}_delete_external_table',
        deletion_dataset_table = f'{gcp_project_id}.{bigquery_dataset}.{external_table_name}',
        ignore_if_missing = True
    )

    return task
