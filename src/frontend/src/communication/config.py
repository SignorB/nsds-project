import os

KAFKA_BOOTSTRAP = f"{os.environ.get('KAFKA_HOST', '100.75.241.112')}:{os.environ.get('KAFKA_PORT', '29092')}"
DEFAULT_RESPONSE_TIMEOUT_S = 30.0