import time
import re
import logging

def log_pre_processing(logger: logging.Logger, log_lines: list[str]) -> list[list[str]]:
    """Removes the header of each log line and groups them in blocks, according to the timeframe between the VLT autoguider stopping an iteration
    and the beggining of the following iteration, which corresponds to an observation period

    Args:
        logger: Current script logging object
        log_lines: List of log lines, in string form

    Returns:
        A list of lists, with each containing a group of tuples, with the id of an observation first and the pre-processed log line last, fitted by observation period
    """

    global tracking_time

    ### Checkpoint - Start log pre-processing
    logger.info(
        "Start log pre-processing: {0}".format(str(time.time() - tracking_time))
    )
    tracking_time = time.time()

    headerless_lines = []
    extraction_flag = False
    num_lines_inserted = 0

    for index, line in enumerate(log_lines):

        # Log line header removal
        header_re = re.compile(
            r"([a-zA-Z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}?)\swt[1-4]tcs\s(.+)(\[[0-9]+\]):\s"
        )

        headerless_line = re.sub(header_re, "", line)

        if headerless_line.find("AG.GUIDE") != -1:
            ### Checkpoint - AG.GUIDE line found
            logger.info(
                "AG.GUIDE line found: {0}".format(str(time.time() - tracking_time))
            )
            tracking_time = time.time()

            if headerless_line.find("STOP") != -1:
                # Start log line extraction
                headerless_lines.append([])
                extraction_flag = True

            elif headerless_line.find("START") != -1:
                # Finish log line extraction
                if len(headerless_lines) != 0:
                    headerless_lines[-1].append((index, headerless_line))

                extraction_flag = False

        if extraction_flag:
            headerless_line = headerless_line.replace("  ", " ")
            headerless_lines[-1].append((index, headerless_line))

            ### Checkpoint - Line added to current list
            logger.info("Line {0} added to current list".format(index))
            tracking_time = time.time()

            num_lines_inserted += 1

    ### Checkpoint - End log pre-processing
    logger.info(
        "End log pre-processing: {0} - Lines pre-processed: {1}".format(
            str(time.time() - tracking_time), str(num_lines_inserted)
        )
    )
    tracking_time = time.time()

    return headerless_lines
