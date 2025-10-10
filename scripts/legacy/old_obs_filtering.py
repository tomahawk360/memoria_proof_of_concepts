import time
import re
import logging

"""
Checks a list with observation periods' log lines and filters out those groups where an observation was not successful.

Args:
    logger: Current script logging object.
    log_lines: List of lists of log lines, grouped by observation period, in string form.
    obs_list: List of tuples, with observation IDs and observation timestamp, in string form.

Returns:
    A list of lists, with each containing a group of headerless log lines, with the id of an observation first and 
    the pre-processed log line last, fitted by observation period with successful observations.
"""


def old_obs_filtering(
    logger: logging.Logger, log_lines: list[list[str]], obs_list: list[list[str]]
) -> list[list[str]]:
    global tracking_time

    ### Checkpoint - Start observation log lines filtering
    logger.info(
        "Start observation log lines filtering: {0}".format(
            str(time.time() - tracking_time)
        )
    )
    tracking_time = time.time()

    obs_log_sections = []
    num_lines_passed = 0

    # Check which log line groups match their timeframes with an observation timestamp
    for log_section in log_lines:
        section_passed_flag = False
        respective_tpl_time = ""

        stop_line = log_section[0][1]
        start_line = log_section[-1][1]

        time_re = re.compile(r"[0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]+)*")

        stop_time = re.search(time_re, stop_line).group()
        start_time = re.search(time_re, start_line).group()

        for _, tpl_time, _ in obs_list:

            obs_time = tpl_time.split("T")[1]

            if stop_time < obs_time and obs_time < start_time:
                respective_tpl_time = obs_time
                section_passed_flag = True
                break

        if section_passed_flag:
            # Save log line group
            obs_log_sections.append(log_section)

            ### Checkpoint - Section within successful observation timeframe found
            logger.info(
                "Section within successful observation timeframe found: {0} - obs_tpl_time: {1}".format(
                    str(time.time() - tracking_time), respective_tpl_time
                )
            )
            tracking_time = time.time()

            num_lines_passed += len(log_section)
        else:
            for _, line in log_section:
                force_line = re.search(r"Executed cmd #([0-9.-]+)", line)
                img_line = re.search(r"EXP NO = ([0-9]+)", line)
                img_line_2 = re.search(
                    r"([0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]+)*)> TEL ACTO (INTTIME)",
                    line,
                )

                if force_line is not None:
                    logger.info(
                        "{0} deleted by obs_filtering".format(force_line.group())
                    )

                elif img_line is not None:
                    logger.info("{0} deleted by obs_filtering".format(img_line.group()))

                elif img_line_2 is not None:
                    logger.info(
                        "{0} deleted by obs_filtering".format(img_line_2.group())
                    )

    ### Checkpoint - End observation log lines filtering
    logger.info(
        "End observation log lines filtering: {0} - Lines passed: {1}".format(
            str(time.time() - tracking_time), str(num_lines_passed)
        )
    )
    tracking_time = time.time()

    return obs_log_sections
