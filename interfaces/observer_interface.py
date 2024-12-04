from abc import ABC, abstractmethod

from job_run_state_management.job_state import JobState


class Observer(ABC):
    """
    The Observer interface declares the update method, used by subjects.
    """
    
    @abstractmethod
    def update(self, job_state: JobState) -> None:
        """
        Receive update from subject.
        """
        pass
    