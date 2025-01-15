from enum import Enum


class EndpointEnum(str, Enum):
    """
    This enum contains all the endpoints in this application.
    """
    CREATE_CONNECTION = "create_connection"
    SUBMIT_JOB = "submit_job"
    SUBMIT_JOB_STATUS = "submit_job_status"
    GENERATE_SUGGESTIONS = "generate_suggestions"
    