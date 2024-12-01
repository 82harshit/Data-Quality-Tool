import logging


class DatabaseUpdateHandler(logging.Handler):
    def __init__(self, job_status_instance):
        super().__init__()
        self.job_status_instance = job_status_instance
        
    def emit(self, record):
        if record.levelno in (logging.INFO, logging.ERROR):
            log_message = self.format(record=record)
            job_status = "IN PROGRESS" if record.levelno == logging.INFO else "ERROR"
            self.job_status_instance.update_in_db(job_status=job_status,status_message=log_message)
            