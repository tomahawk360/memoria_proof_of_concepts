import time
import re
from datetime import datetime, timedelta
import logging

def obs_filtering(
    logger: logging.Logger, log_lines: list[str], obs_list: list[list[str]], date: str
) -> dict:
    """Creates time blocks with the observation log lines, according by observation instrument, which are later used to filter the log
    lines that fit within the range of said blocks

    Args:
        logger: Current script logging object
        log_lines: List of lists of log lines, grouped by observation period, in string form
        obs_list: List of tuples, with observation IDs and observation timestamp, in string form
        date: Date, in string form, of the log file

    Returns:
        A list of lists, with each containing a group of headerless log lines, fitted by observation period and used observation instrument
    """

    global tracking_time

    ### Checkpoint - Start new observation log lines filtering
    logger.info(
        "Start new observation log lines filtering: {0}".format(
            str(time.time() - tracking_time)
        )
    )
    tracking_time = time.time()

    obs_time_intervals = {}
    obs_data_intervals = {}

    num_lines_passed = 0

    ### Parameters
    lower_margin = 10.0
    upper_margin = 10.0
    join_threshold = 30.0

    ### Filter observation file by instrument
    for tpl_instrument, tpl_date, exptime in obs_list:

        ### Create time blocks with TLP_START and EXPTIME
        if tpl_instrument not in obs_time_intervals.keys():
            obs_time_intervals[tpl_instrument] = []

        obs_time_intervals[tpl_instrument].append(
            [
                datetime.strptime(tpl_date, "%Y-%m-%dT%H:%M:%S")
                - timedelta(seconds=lower_margin),
                datetime.strptime(tpl_date, "%Y-%m-%dT%H:%M:%S")
                + timedelta(seconds=float(exptime) + upper_margin),
            ]
        )

    for instrument in obs_time_intervals.keys().__iter__():
        interval_list = obs_time_intervals[instrument]

        interval_list.sort()

        interval_index = 0

        ### Join intervals that overlap (within the given threshlod)
        while interval_index < len(interval_list) - 1:
            curr_interval = interval_list[interval_index]
            next_interval = interval_list[interval_index + 1]

            if curr_interval[1] >= next_interval[0] - timedelta(seconds=join_threshold):
                interval_list[interval_index][1] = next_interval[1]
                interval_list.remove(next_interval)

            else:
                interval_index += 1

        ### Use each time block to filter the log lines
        section_log = []
        datetime_date = datetime.strptime(date, "%Y-%m-%d")

        for line in log_lines:
            date_format = re.search(
                r"([A-Za-z]{3} [0-9]+ [0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]+)*)", line
            )

            if date_format is not None:
                line_date_str = date_format.group()
                line_date_str = str(datetime_date.year) + " " + line_date_str

                line_date = datetime.strptime(line_date_str, "%Y %b %d %H:%M:%S")

                for interval_init, interval_stop in interval_list:
                    if interval_init <= line_date and line_date <= interval_stop:
                        num_lines_passed += 1

                        section_log.append(line)
                        break

        obs_data_intervals[instrument] = section_log

        # print(instrument + " : " + str(len(interval_list)))

    ### Checkpoint - End new observation log lines filtering
    logger.info(
        "End new observation log lines filtering: {0} - Lines passed: {1}".format(
            str(time.time() - tracking_time), str(num_lines_passed)
        )
    )
    tracking_time = time.time()

    return obs_data_intervals