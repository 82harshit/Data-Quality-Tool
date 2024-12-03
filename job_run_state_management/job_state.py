from logging_config import dqt_logger


class JobState:
    def __init__(self, job_id):
        self.job_id = job_id
        self.job_status = None
        self.status_message = None
        self.observers = []

    def add_observer(self, observer):
        self.observers.append(observer)

    def notify_observers(self):
        for observer in self.observers:
            observer.update(self)

    def set_state(self, status, message=""):
        self.job_status = status
        self.status_message = message
        dqt_logger.info(f"State Updated: {status} - {message}")
        self.notify_observers()
        

# Context manager for automatic state updates
class JobStateManager:
    def __init__(self, job_state, start_message="Starting job...", end_message="Job completed"):
        self.job_state = job_state
        self.start_message = start_message
        self.end_message = end_message

    def __enter__(self):
        self.job_state.set_state("Started", self.start_message)
        return self.job_state  # Return the state object for use inside the block

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.job_state.set_state("Failed", str(exc_val))
        else:
            self.job_state.set_state("Completed", self.end_message)