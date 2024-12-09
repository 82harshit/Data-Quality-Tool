import os
from datetime import datetime
import logging.config

# Create logs directory if it doesn't exist
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)

# Generate log file name based on current date
log_filename = os.path.join(log_dir, datetime.now().strftime('%Y-%m-%d') + '.log')

# Define the logging configuration
logging_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simpleFormatter': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        },
    },
    'handlers': {
        'consoleHandler': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'simpleFormatter',
            'stream': 'ext://sys.stdout',
        },
        'fileHandler': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'level': 'INFO',
            'formatter': 'simpleFormatter',
            'filename': log_filename,
            'when': 'midnight',  # Rotate at midnight
            'interval': 1,      # Rotate every day
            'backupCount': 7,   # Keep 7 backup log files
            'encoding': 'utf-8',
        },
    },
    'loggers': {
        'root': {
            'level': 'INFO',
            'handlers': ['consoleHandler', 'fileHandler'],
            'propagate': False,
        },
        'dqt_logger': {
            'level': 'INFO',
            'handlers': ['consoleHandler', 'fileHandler'],
            'propagate': False,
        },
        'watchfiles': {
            'level': 'WARNING',
            'handlers': [],
            'propogate': False,
        }
    },
}

logging.config.dictConfig(logging_config)

dqt_logger = logging.getLogger('dqt_logger')
