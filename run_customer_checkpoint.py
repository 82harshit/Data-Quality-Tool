"""
This is a basic generated Great Expectations script that runs a Checkpoint.

Checkpoints are the primary method for validating batches of data in production and triggering any followup actions.

A Checkpoint facilitates running a validation as well as configurable Actions such as updating Data Docs, sending a
notification to team members about validation results, or storing a result in a shared cloud storage.

See also <cyan>https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_create_a_new_checkpoint_using_test_yaml_config.html</cyan> for more information about the Checkpoints and how to configure them in your Great Expectations environment.

Checkpoints can be run directly without this script using the `great_expectations checkpoint run` command.  This script
is provided for those who wish to run Checkpoints in python.

Usage:
- Run this file: `python great_expectations/uncommitted/run_customer_checkpoint.py`.
- This can be run manually or via a scheduler such, as cron.
- If your pipeline runner supports python snippets, then you can paste this into your pipeline.
"""

from great_expectations.checkpoint.types.checkpoint_result import (
    CheckpointResult,
)
from great_expectations.data_context import FileDataContext, get_context

data_context: FileDataContext = get_context(context_root_dir="C:\Data Quality Tool\gx")

def run_checkpoint(run_name):
    result: CheckpointResult = data_context.run_checkpoint(
        checkpoint_name="customer_checkpoint",
        batch_request=None,
        run_name=run_name,
    )

    # if not result["success"]:
    #     print("Validation failed!")
    #     return None
    
    # print("Validation succeeded!")
    return result

# run_checkpoint("test_run")