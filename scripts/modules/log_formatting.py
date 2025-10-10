import time
import re
import logging

def log_formatting(logger: logging.Logger, log_lines: list[str]) -> list[str]:
    """Removes the header and clears problematic characters of each log line

    Args:
        logger: Current script logging object
        log_lines: List of log lines, in string form

    Returns:
        A list of pre-processed log lines in string format
    """

    global tracking_time

    ### Checkpoint - Start log formatting
    logger.info("Start log formatting: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    headerless_lines = []

    for _, line in enumerate(log_lines):

        # Log line header removal
        header_re = re.compile(
            r"([a-zA-Z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}?)\swt[1-4]tcs\s(.+)(\[[0-9]+\]):\s"
        )

        headerless_line = re.sub(header_re, "", line)

        headerless_line = headerless_line.replace("  ", " ")
        headerless_lines.append(headerless_line)

        ### Checkpoint - Line added to current list
        logger.info(
            "Line added to current list: {0}".format(str(time.time() - tracking_time))
        )
        tracking_time = time.time()

    ### Checkpoint - End log formatting
    logger.info("End log formatting: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    return headerless_lines
