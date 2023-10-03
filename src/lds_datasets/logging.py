"""Logging configuration."""
import logging
import sys
from pathlib import Path

import structlog
from structlog.processors import CallsiteParameter
from structlog.types import Processor


def setup_logging(
    json_logs: bool = True, log_level: str = "info", logfile: str = ""
) -> None:
    """Get a structlog-based logger.

    This configuration is built for console rendering, allowing a user to easily
    configure logging.
    """
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        # Add the name of the logger to event dict.
        structlog.stdlib.add_logger_name,
        # Add log level to event dict.
        structlog.stdlib.add_log_level,
        # Perform %-style formatting.
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.stdlib.ExtraAdder(),
        # Add a timestamp in ISO 8601 format.
        structlog.processors.TimeStamper(fmt="iso"),
        # If some value is in bytes, decode it to a unicode str.
        structlog.processors.UnicodeDecoder(),
        # Add callsite parameters.
        structlog.processors.CallsiteParameterAdder(
            parameters=(
                CallsiteParameter.FILENAME,
                CallsiteParameter.FUNC_NAME,
                CallsiteParameter.LINENO,
            )
        ),
    ]

    if json_logs:
        # If the "exc_info" key in the event dict is either true or a sys.exc_info() tuple, remove
        # "exc_info" and render the exception with traceback into the "exception" key. We want to
        # pretty-print for console, so only format it for json.
        shared_processors.append(structlog.processors.format_exc_info)
        shared_processors.append(structlog.processors.dict_tracebacks)

    # Basic structlog configuration to filter by level and play nice with console.
    structlog.configure(
        processors=shared_processors
        + [
            # Prepare event dict for `ProcessorFormatter`.
            # This is placed here because it must always be last.
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        # Effectively freeze configuration after creating the first bound
        # logger.
        cache_logger_on_first_use=True,
    )

    log_renderer: Processor
    if json_logs:
        log_renderer = structlog.processors.JSONRenderer()
    else:
        log_renderer = structlog.dev.ConsoleRenderer(
            exception_formatter=structlog.dev.plain_traceback
        )

    formatter = structlog.stdlib.ProcessorFormatter(
        # These run ONLY on `logging` entries that do NOT originate within
        # structlog.
        foreign_pre_chain=shared_processors,
        # These run on ALL entries after the pre_chain is done.
        processors=[
            # Remove _record & _from_structlog.
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            log_renderer,
        ],
    )

    handler = logging.StreamHandler()
    # Use OUR `ProcessorFormatter` to format all `logging` entries.
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()

    # Don't duplicate handlers if we call this function twice
    if not root_logger.hasHandlers():
        root_logger.addHandler(handler)

    if logfile:
        # Define the path for which you want to create parent directories
        logfile_path = Path(logfile)
        # Ensure the parent directories exist
        logfile_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(logfile)
        root_logger.addHandler(file_handler)

    root_logger.setLevel(log_level.upper())

    def handle_exception(  # type: ignore[no-untyped-def]
        exc_type, exc_value, exc_traceback
    ) -> None:
        """Log any uncaught exception instead of letting it be printed by Python.

        (but leave KeyboardInterrupt untouched to allow users to Ctrl+C to stop)
        See: https://stackoverflow.com/a/16993115/3641865
        """
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        root_logger.error(
            "Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback)
        )

    sys.excepthook = handle_exception
