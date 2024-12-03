class DatabaseObserver:
    def __init__(self, job_run_status_instance):
        self.job_run_status_instance = job_run_status_instance
        self.job_run_status_instance.connect_to_db()
        
    def update(self, job_state):
        self.job_run_status_instance.update_in_db(job_state.job_status, job_state.status_message)
        
    def __del__(self):
        self.job_run_status_instance.close_db_connection()