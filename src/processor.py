"""Processing stage entry point: runs the engine chosen by PROCESSOR (spark or flink)."""

import sys
import config
from common.logger import get_logger

logger = get_logger("processor")

if __name__ == "__main__":
    try:
        logger.info(f"Processor started: engine={config.PROCESSOR}")
        config.build_processor().run()
    except KeyboardInterrupt:
        logger.info("Stopping processor...")
    except Exception:
        logger.exception("Processor crashed with an unhandled exception")
        sys.exit(1)
