class JobIDSingleton:
    _instance = None
    _job_id = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def set_job_id(cls, job_id):
        cls._job_id = job_id

    @classmethod
    def get_job_id(cls):
        return cls._job_id