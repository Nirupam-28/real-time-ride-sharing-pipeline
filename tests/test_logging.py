
from common.logging_config import setup_logging

logger = setup_logging("logging_test")

logger.debug("Debug message")
logger.info("Logging system initialized")
logger.warning("This is a warning")
logger.error("This is a sample error")

print("Logging test completed.")
