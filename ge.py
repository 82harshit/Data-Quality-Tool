import datetime
from wsgiref.validate import validator

import pandas as pd

import great_expectations as gx
from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource
from great_expectations.core.batch import BatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.exceptions import DataContextError

context = gx.get_context()

def create_new_datasource(datasource_name):
    datasource_yaml = f"""
    name: {datasource_name}
    class_name: Datasource
    execution_engine:
    class_name: PandasExecutionEngine
    data_connectors:
    default_inferred_data_connector_name:
        class_name: InferredAssetFilesystemDataConnector
        base_directory: data_source
        default_regex:
        group_names:
            - data_asset_name
        pattern: (.*)
    default_runtime_data_connector_name:
        class_name: RuntimeDataConnector
        assets:
        my_runtime_asset_name:
            batch_identifiers:
            - runtime_batch_identifier_name
    """
    context.test_yaml_config(yaml_config=datasource_yaml)
    sanitize_yaml_and_save_datasource(context, datasource_yaml, overwrite_existing=False)
    # context.list_datasources()

batch_request = {'datasource_name': 'customer_100', 'data_connector_name': 'default_inferred_data_connector_name', 'data_asset_name': 'customers-100.csv', 'limit': 1000}

def create_expectation_suite(expectation_suite_name):
    try:
        suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
        print(f'Loaded ExpectationSuite "{suite.expectation_suite_name}" containing {len(suite.expectations)} expectations.')
    except DataContextError:
        suite = context.add_expectation_suite(expectation_suite_name=expectation_suite_name)
        print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')


def create_validator(expectation_suite_name):
    validator = context.get_validator(
        batch_request=BatchRequest(**batch_request),
        expectation_suite_name=expectation_suite_name
    )

    validator.save_expectation_suite(discard_failed_expectations=False)
    return validator

def run_checkpoint(expectation_suite_name, validator):
    checkpoint_config = {
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": batch_request,
                "expectation_suite_name": expectation_suite_name
            }
        ]
    }

    checkpoint = SimpleCheckpoint(
        f"{validator.active_batch_definition.data_asset_name}_{expectation_suite_name}",
        context,
        **checkpoint_config
    )

    checkpoint_result = checkpoint.run()
    return checkpoint_result

# context.build_data_docs()

expectation_suite_name = create_expectation_suite("")
validator = create_validator(expectation_suite_name)
checkpoint_result = run_checkpoint(expectation_suite_name,validator)

validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]
context.open_data_docs(resource_identifier=validation_result_identifier)