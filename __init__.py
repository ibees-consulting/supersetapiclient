import sys
sys.dont_write_bytecode = True

# Configure logging
import logging

from .client import SupersetClient  # noqa

FORMAT = "[%(asctime)-15s] %(levelname)s:%(name)s - %(message)s"

logging.basicConfig(format=FORMAT)
