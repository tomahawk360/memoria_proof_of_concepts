from astropy.io import fits
from datetime import datetime, timedelta
from io import StringIO
import os
import re
import math
import json
import csv
import time
import xml
import logging
import pandas as pd
import requests
import csv
import argparse
import sys

# from memory_profiler import profile
# from ttp import ttp


"""
Global variables:
    tracking_time: Initialization of time for checkpoint monitoring
"""
tracking_time = time.time()

"""
Opens the VLT Observations CSV file and filters out the header and irrelevant fields.

Args:
    arch_name: CSV file relative path.

Returns:
    A list of lists, with each lists containing the observation instrument, it's respective timestamp and the exptime.
"""


def open_obs_file(arch_name) -> list[list[str]]:
    tpl_list = []

    with open(arch_name, mode="r") as query:
        reader = csv.reader(query)

        next(reader)

        for line in reader:
            if line[11] != "":
                tpl_list.append([line[10].split("_")[0], line[11], line[12]])

    tpl_list.pop(0)

    return tpl_list


"""
Opens a text file and deposits it's information into a list.

Args:
    arch_name: Text file relative path.

Returns:
    A list with all the lines as strings.
"""


def open_txt_file(arch_name) -> list[str]:
    with open(file=arch_name, mode="r", encoding="iso-8859-1") as logs:
        lines = logs.readlines()

    return lines


"""
Fetch the VLT Observations CSV file from the ESO raw database and saves it in the obs_files folder
Args:
    destination_folder: Relative path of the folder where the fetched CSV file is stored.
    obs_date: Date of the CSV file to be fetched.

Returns:
    Status code of the fetch
"""


def fetch_obs_file(destination_folder, obs_date) -> int:
    url = "https://archive.eso.org/wdb/wdb/eso/eso_archive_main/query"

    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0) Gecko/20100101 Firefox/142.0",
        "Referer": "https://archive.eso.org/eso/eso_archive_main.html",
    }

    payload = {
        "wdbo": (None, "csv/download"),
        "max_rows_returned": (None, "999999"),
        "instrument": (None, ""),
        "tab_object": (None, "on"),
        "target": (None, ""),
        "resolver": (None, "simbad"),
        "ra": (None, ""),
        "dec": (None, ""),
        "box": (None, "00 10 00"),
        "degrees_or_hours": (None, "hours"),
        "tab_target_coord": (None, "on"),
        "format": (None, "SexaHour"),
        "wdb_input_file": (None, ""),
        "night": (None, obs_date),
        "stime": (None, ""),
        "starttime": (None, "12"),
        "etime": (None, ""),
        "endtime": (None, "12"),
        "tab_prog_id": (None, "on"),
        "prog_id": (None, ""),
        "gto": (None, ""),
        "pi_coi": (None, ""),
        "obs_mode": (None, ""),
        "title": (None, ""),
        "image[]": (None, "EFOSC;'IMA%'"),
        "image[]": (None, "EMMI;'IMA%'"),
        "image[]": (None, "ERIS"),
        "image[]": (None, "FORS1;'IMA%'"),
        "image[]": (None, "FORS2;'IMA%'"),
        "image[]": (None, "HAWKI;'IMA%'"),
        "image[]": (None, "GROND"),
        "image[]": (None, "ISAAC;'IMA%'"),
        "image[]": (None, "NAOS+CONICA;'IMA%','SDI%'"),
        "image[]": (None, "OMEGACAM"),
        "image[]": (None, "SOFI;'IMA%'"),
        "image[]": (None, "SPHERE;'IMA%'"),
        "image[]": (None, "SUSI"),
        "image[]": (None, "TIMMI;'IMA%'"),
        "image[]": (None, "VIMOS;'IMA%'"),
        "image[]": (None, "VIRCAM"),
        "image[]": (None, "VISIR;'IMA%'"),
        "image[]": (None, "WFI"),
        "image[]": (None, "XSHOOTER;'IMA%'"),
        "spectrum[]": (None, "CES"),
        "spectrum[]": (None, "CRIRE;'SPECTRUM%'"),
        "spectrum[]": (None, "EFOSC;'SPECTRUM%'"),
        "spectrum[]": (None, "EMMI;'SPECTRUM%','ECHELLE%','MOS%'"),
        "spectrum[]": (None, "ERIS;'IFU%','%LSS%'"),
        "spectrum[]": (None, "ESPRESSO"),
        "spectrum[]": (None, "FEROS"),
        "spectrum[]": (None, "FORS1;'SPECTRUM%','MOS%','IMAGE_SPECTRUM%'"),
        "spectrum[]": (
            None,
            "FORS2;'SPECTRUM%','ECHELLE%','MOS%','MXU%','HIT%','IMAGE_SPECTRUM%'",
        ),
        "spectrum[]": (None, "GIRAF"),
        "spectrum[]": (None, "HARPS"),
        "spectrum[]": (None, "ISAAC;'SPECTRUM%'"),
        "spectrum[]": (None, "KMOS"),
        "spectrum[]": (None, "MUSE"),
        "spectrum[]": (None, "NAOS+CONICA;'SPECTRUM%'"),
        "spectrum[]": (None, "NIRPS"),
        "spectrum[]": (None, "SINFO"),
        "spectrum[]": (None, "SOFI;'SPECTRUM%'"),
        "spectrum[]": (None, "SPHERE;'IFU%','SPECTRUM%'"),
        "spectrum[]": (None, "TIMMI;'SPECTRUM%'"),
        "spectrum[]": (None, "UVES"),
        "spectrum[]": (None, "VIMOS;'IFU%','MOS%'"),
        "spectrum[]": (None, "VISIR;'SPECTRUM%','ECHELLE%'"),
        "spectrum[]": (None, "SHOOT"),
        "vlti[]": (None, "AMBER"),
        "vlti[]": (None, "GRAVITY"),
        "vlti[]": (None, "MATISSE"),
        "vlti[]": (None, "MIDI"),
        "vlti[]": (None, "PIONIER"),
        "vlti[]": (None, "VINCI"),
        "polarim[]": (None, "EFOSC;'POLARIM%'"),
        "polarim[]": (None, "FORS1;'POLARIM%'"),
        "polarim[]": (None, "FORS2;'POLARIM%'"),
        "polarim[]": (None, "ISAAC;'POLARIM%'"),
        "polarim[]": (None, "NAOS+CONICA;'POLARIM%'"),
        "polarim[]": (None, "SOFI;'POLARIM%'"),
        "polarim[]": (None, "SPHERE;'POLARIM%'"),
        "corono[]": (None, "EFOSC;'%CORO%'"),
        "corono[]": (None, "ERIS;'CORO%'"),
        "corono[]": (None, "NAOS+CONICA;'%CORO%'"),
        "corono[]": (None, "SPHERE;'%CORO%'"),
        "corono[]": (None, "VISIR;'%CORO%'"),
        "other[]": (None, "ALPACA"),
        "other[]": (None, "APICAM"),
        "other[]": (None, "APEXBOL"),
        "other[]": (None, "APEXHET"),
        "other[]": (None, "FAIM6"),
        "other[]": (None, "FAIM7"),
        "other[]": (None, "GRIPS19"),
        "other[]": (None, "LGSF"),
        "other[]": (None, "MAD"),
        "other[]": (None, "MASCOT"),
        "other[]": (None, "SPECU"),
        "other[]": (None, "WFCAM"),
        "sam[]": (None, "ERIS;'%SAM%'"),
        "sam[]": (None, "NAOS+CONICA;'%SAM%'"),
        "sam[]": (None, "SPHERE;'%SAM%'"),
        "sam[]": (None, "VISIR;'SAM%'"),
        "tab_dp_cat": (None, "on"),
        "dp_cat": (None, "SCIENCE"),
        "dp_cat": (None, "ACQUISITION"),
        "tab_dp_type": (None, "on"),
        "dp_type": (None, ""),
        "dp_type_user": (None, ""),
        "tab_dp_tech": (None, "on"),
        "dp_tech": (None, ""),
        "dp_tech_user": (None, ""),
        "tab_dp_id": (None, "on"),
        "dp_id": (None, ""),
        "origfile": (None, ""),
        "tab_rel_date": (None, "on"),
        "rel_date": (None, ""),
        "obs_name": (None, ""),
        "ob_id": (None, ""),
        "tab_tpl_start": (None, "on"),
        "tpl_start": (None, ""),
        "tab_tpl_id": (None, "on"),
        "tpl_id": (None, ""),
        "tab_exptime": (None, "on"),
        "exptime": (None, ""),
        "tab_filter_path": (None, "on"),
        "filter_path": (None, ""),
        "tab_wavelength_input": (None, "on"),
        "wavelength_input": (None, ""),
        "tab_fwhm_input": (None, "on"),
        "fwhm_input": (None, ""),
        "gris_path": (None, ""),
        "grat_path": (None, ""),
        "slit_path": (None, ""),
        "tab_instrument": (None, "on"),
        "add": (
            None,
            "((ins_id like 'EFOSC%' AND (dp_tech like 'IMA%')) or (ins_id like 'EMMI%' AND (dp_tech like 'IMA%')) or (ins_id like 'ERIS%') or (ins_id like ('ERIS%') AND ((dp_tech like 'IMA%') AND (dp_tech not like '%SAM%'))) or (ins_id like 'FORS1%' AND (dp_tech like 'IMA%')) or (ins_id like 'FORS2%' AND (dp_tech like 'IMA%')) or (ins_id like 'HAWKI%' AND (dp_tech like 'IMA%')) or (ins_id like 'GROND%') or (ins_id like 'ISAAC%' AND (dp_tech like 'IMA%')) or (ins_id like 'NAOS+CONICA%' AND (dp_tech like 'IMA%' OR dp_tech like 'SDI%')) or (ins_id like 'OMEGACAM%') or (ins_id like 'SOFI%' AND (dp_tech like 'IMA%')) or (ins_id like 'SPHERE%' AND (dp_tech like 'IMA%')) or (ins_id like 'SUSI%') or (ins_id like 'TIMMI%' AND (dp_tech like 'IMA%')) or (ins_id like 'VIMOS%' AND (dp_tech like 'IMA%')) or (ins_id like 'VIRCAM%') or (ins_id like 'VISIR%' AND (dp_tech like 'IMA%')) or (ins_id like 'WFI%') or (ins_id like 'XSHOOTER%' AND (dp_tech like 'IMA%')) or (ins_id like 'CES%') or (ins_id like 'CRIRE%' AND ((dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'EFOSC%' AND ((dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'EMMI%' AND ((dp_tech like 'SPECTRUM%' OR dp_tech like 'ECHELLE%' OR dp_tech like 'MOS%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'ERIS%' AND ((dp_tech like 'IFU%' OR dp_tech like '%LSS%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'ESPRESSO%') or (ins_id like 'FEROS%') or (ins_id like 'FORS1%' AND ((dp_tech like 'SPECTRUM%' OR dp_tech like 'MOS%' OR dp_tech like 'IMAGE_SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'FORS2%' AND ((dp_tech like 'SPECTRUM%' OR dp_tech like 'ECHELLE%' OR dp_tech like 'MOS%' OR dp_tech like 'MXU%' OR dp_tech like 'HIT%' OR dp_tech like 'IMAGE_SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'GIRAF%') or (ins_id like 'HARPS%') or (ins_id like 'ISAAC%' AND ((dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'KMOS%') or (ins_id like 'MUSE%') or (ins_id like 'NAOS+CONICA%' AND ((dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'NIRPS%') or (ins_id like 'SINFO%') or (ins_id like 'SOFI%' AND ((dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'SPHERE%' AND ((dp_tech like 'IFU%' OR dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'TIMMI%' AND ((dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'UVES%') or (ins_id like 'VIMOS%' AND ((dp_tech like 'IFU%' OR dp_tech like 'MOS%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'VISIR%' AND ((dp_tech like 'SPECTRUM%' OR dp_tech like 'ECHELLE%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'SHOOT%') or (ins_id in ('SHOOT','XSHOOTER') AND ((dp_tech like 'ECHELLE%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'AMBER%') or (ins_id like 'GRAVITY%') or (ins_id like 'MATISSE%') or (ins_id like 'MIDI%') or (ins_id like 'PIONIER%') or (ins_id like 'VINCI%') or (ins_id like 'EFOSC%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'FORS1%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'FORS2%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'ISAAC%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'NAOS+CONICA%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'SOFI%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'SPHERE%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'EFOSC%' AND (dp_tech like '%CORO%')) or (ins_id like 'ERIS%' AND (dp_tech like 'CORO%')) or (ins_id like 'NAOS+CONICA%' AND (dp_tech like '%CORO%')) or (ins_id like 'SPHERE%' AND (dp_tech like '%CORO%')) or (ins_id like 'VISIR%' AND (dp_tech like '%CORO%')) or (ins_id like 'ALPACA%') or (ins_id like 'APICAM%') or (ins_id like 'APEXBOL%') or (ins_id like 'APEXHET%') or (ins_id like 'FAIM6%') or (ins_id like 'FAIM7%') or (ins_id like 'GRIPS19%') or (ins_id like 'LGSF%') or (ins_id like 'MAD%') or (ins_id like 'MASCOT%') or (ins_id like 'SPECU%') or (ins_id like 'WFCAM%') or (ins_id like 'ERIS%' AND (dp_tech like '%SAM%')) or (ins_id like 'NAOS+CONICA%' AND (dp_tech like '%SAM%')) or (ins_id like 'SPHERE%' AND (dp_tech like '%SAM%')) or (ins_id like 'VISIR%' AND (dp_tech like 'SAM%')))",
        ),
        "tab_tel_airm_start": (None, "on"),
        "tab_stat_instrument": (None, "on"),
        "tab_ambient": (None, "on"),
        "tab_stat_exptime": (None, "on"),
        "tab_HDR": (None, "on"),
        "tab_mjd_obs": (None, "on"),
        "aladin_colour": (None, "aladin_instrument"),
        "tab_stat_plot": (None, "on"),
        "order": (None, ""),
    }

    # Make a POST request with data
    response = requests.post(url, files=payload, headers=header)

    # Access the response body as CSV
    if response.status_code == 200:
        obs_csv_list = []

        csv_data = StringIO(response.text)
        reader = csv.reader(csv_data)

        for row in reader:
            if len(row) > 1:
                obs_csv_list.append(row)

        with open(
            "{0}/{1}.csv".format(destination_folder, obs_date), "w", newline=""
        ) as csvfile:
            writer = csv.writer(csvfile)

            # Write the header row
            writer.writerow(obs_csv_list[0])

            # Write the remaining data rows
            writer.writerows(obs_csv_list[1:])

    else:
        print("Error while fetching observation csv ", response.status_code)

    return response.status_code


"""
Removes the header of each log line and groups them in blocks, according to the timeframe between the VLT autoguider stopping an iteration
and the beggining of the following iteration, which corresponds to an observation period.

Args:
    logger: Current script logging object.
    log_lines: List of log lines, in string form.

Returns:
    A list of lists, with each containing a group of tuples, with the id of an observation first and 
    the pre-processed log line last, fitted by observation period.
"""


def log_pre_processing(logger: logging.Logger, log_lines: list[str]) -> list[list[str]]:
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


"""
Removes the header and clears problematic characters of each log line.
Args:
    logger: Current script logging object.
    log_lines: List of log lines, in string form.

Returns:
    A list of pre-processed log lines in string format.
"""


def log_formatting(logger: logging.Logger, log_lines: list[str]) -> list[str]:
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


def obs_filtering(
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


"""
Creates time blocks with the observation log lines, according by observation instrument, which are later used to filter the log
lines that fit within the range of said blocks.

Args:
    logger: Current script logging object.
    log_lines: List of lists of log lines, grouped by observation period, in string form.
    obs_list: List of tuples, with observation IDs and observation timestamp, in string form.
    date: Date, in string form, of the log file.

Returns:
    A list of lists, with each containing a group of headerless log lines, fitted by observation period and used observation instrument.
"""


def new_obs_filtering(
    logger: logging.Logger, log_lines: list[str], obs_list: list[list[str]], date: str
) -> dict:
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


"""
Parsess each log line and extracts their dynamic data, using the Text Template Parsing library (ttp).

Args:
    logger: Current script logging object.
    log_sections: List of lists of log lines, grouped by successful observation period, in string form.
    templates_list: List of log line parsing templates, in string form.

Returns:
    A list of dictionaries, with each containing the dynamic data extracted from a log line.
"""


def log_parsing_ttp(
    logger: logging.Logger, log_sections: list[list[str]], templates_list: list[str]
) -> list[dict]:
    global tracking_time

    ### Checkpoint - Start log line parsing
    logger.info("Start log line parsing: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    parsed_data = []

    for log_section in log_sections:
        for index, line in log_section:

            # Iterates through all templates
            for template in templates_list:

                # Log line parsing
                try:
                    # Parsing with Text Template Parsing library
                    parser = ttp(data=line, template=template)
                    parser.parse()
                    key = list(
                        parser.result(template="per_input", structure="dictionary")
                    )[0]
                    result = parser.result(
                        template="per_input", structure="dictionary"
                    )[key][0]

                    # If match is true, the data extracted is saved and jumps into the next log line
                    if len(result) != 0:
                        # Save parsing result
                        result["Num_linea"] = index
                        parsed_data.append(result)
                        break

                    ### Checkpoint - Log line parsing attempt
                    logger.info(
                        "Log line parsing attempt: {0}".format(
                            str(time.time() - tracking_time)
                        )
                    )
                    tracking_time = time.time()

                # Recolecta otros errores sobre la plantilla usada (se revisarán a posteriori)
                except xml.etree.ElementTree.ParseError as err:
                    logger.info("ERROR: " + str(err))

            ### Checkpoint - End iteration on template list
            logger.info(
                "End iteration on template list: {0}".format(
                    str(time.time() - tracking_time)
                )
            )
            tracking_time = time.time()

        ### Checkpoint - End iteration on log line group
        logger.info(
            "End iteration on log line group: {0}".format(
                str(time.time() - tracking_time)
            )
        )
        tracking_time = time.time()

    ### Checkpoint - End log line parsing
    logger.info("End log line parsing: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    return parsed_data


"""
Parsess each log line and extracts their dynamic data, using regular expressions.

Args:
    logger: Current script logging object.
    log_sections: List of log lines, grouped by successful observation period, in string form.
    templates_list: List of log line parsing templates, in string form.

Returns:
    A list of dictionaries, with each containing the dynamic data extracted from a log line.
"""


def log_parsing_regex(
    logger: logging.Logger, log_sections: dict, templates_list: list[str]
) -> list[dict]:
    global tracking_time

    ### Checkpoint - Start log line parsing
    logger.info("Start log line parsing: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    parsed_data = []
    num_lines_parsed = 0

    for log_section_key in log_sections.keys().__iter__():
        for line in log_sections[log_section_key]:
            result = {}
            parsing_flag = False

            # Parses keywords
            check_forces = re.search(r"SetGlb[Abs|Rel]", line)
            check_exp_no = re.search(r"EXP NO", line)
            check_inttime = re.search(r"INTTIME", line)
            check_add_data = re.search(r"TEL", line)

            if check_forces is not None:
                # print(line)
                check_f_dist = re.search(r"Forces", line)
                check_f_header = re.search(r"Executed", line)
                check_f_init = re.search(r"Received", line)

                if check_f_dist is not None:
                    template = templates_list[0]
                    template = template.replace("\n", "")

                    # Parsing with regular expressions
                    parser = re.search(template, line)

                    # If match is true, the data extracted is saved and jumps into the next log line
                    if parser is not None:
                        parsing_flag = True

                        result["group"] = "FORCES"
                        result["label"] = "f_dist"
                        result["date"] = parser.group(2)
                        result["time"] = parser.group(3)
                        result["data"] = parser.group(6)

                elif check_f_header is not None:
                    template = templates_list[1]
                    template = template.replace("\n", "")

                    # Parsing with regular expressions
                    parser = re.search(template, line)

                    # If match is true, the data extracted is saved and jumps into the next log line
                    if parser is not None:
                        parsing_flag = True

                        result["group"] = "FORCES"
                        result["label"] = "f_id"
                        result["date"] = parser.group(2)
                        result["time"] = parser.group(3)
                        result["data"] = parser.group(5)

                elif check_f_init is not None:
                    template = templates_list[2]
                    template = template.replace("\n", "")

                    # Parsing with regular expressions
                    parser = re.search(template, line)

                    # If match is true, the data extracted is saved and jumps into the next log line
                    if parser is not None:
                        parsing_flag = True

                        result["time"] = parser.group(3)
                        result["data"] = parser.group(5)
                        result["group"] = "INIT"

            elif check_exp_no is not None:
                template = templates_list[3]
                template = template.replace("\n", "")

                # Parsing with regular expressions
                parser = re.search(template, line)

                # If match is true, the data extracted is saved and jumps into the next log line
                if parser is not None:
                    parsing_flag = True

                    result["time"] = parser.group(2)
                    result["group"] = "IMAGE"
                    result["label"] = parser.group(5)
                    result["data"] = parser.group(4)

            elif check_inttime is not None:
                template = templates_list[4]
                template = template.replace("\n", "")

                # Parsing with regular expressions
                parser = re.search(template, line)

                # If match is true, the data extracted is saved and jumps into the next log line
                if parser is not None:
                    parsing_flag = True

                    result["time"] = parser.group(2)
                    result["group"] = "IMAGE"
                    result["label"] = parser.group(4)
                    result["data"] = parser.group(5)

            elif check_add_data is not None:
                for template_ind in range(5, len(templates_list)):
                    template = templates_list[template_ind]
                    template = template.replace("\n", "")

                    # Parsing with regular expressions
                    parser = re.search(template, line)

                    # If match is true, the data extracted is saved and jumps into the next log line
                    if parser is not None:
                        parsing_flag = True

                        result["time"] = parser.group(2)
                        result["group"] = parser.group(4)
                        result["label"] = parser.group(5)
                        result["data"] = parser.group(6)

                        break

            # Save parsing result
            if parsing_flag:
                parsed_data.append(result)

                ### Checkpoint - Log line parsing successful
                logger.info(
                    "Log line parsing succesful: {0}".format(
                        str(time.time() - tracking_time)
                    )
                )
                tracking_time = time.time()

                num_lines_parsed += 1

            else:
                force_line = re.search(r"Executed cmd #([0-9.-]+)", line)
                img_line = re.search(r"EXP NO = ([0-9]+)", line)
                img_line_2 = re.search(
                    r"([0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]+)*)> TEL ACTO (INTTIME)",
                    line,
                )

                if force_line is not None:
                    logger.info(
                        "{0} deleted by log_parsing_regex".format(force_line.group())
                    )

                elif img_line is not None:
                    logger.info(
                        "{0} deleted by log_parsing_regex".format(img_line.group())
                    )

                elif img_line_2 is not None:
                    logger.info(
                        "{0} deleted by log_parsing_regex".format(img_line_2.group())
                    )

    ### Checkpoint - End log line parsing
    logger.info(
        "End log line parsing: {0} - Lines parsed: {1}".format(
            str(time.time() - tracking_time), str(num_lines_parsed)
        )
    )
    tracking_time = time.time()

    return parsed_data


"""
Generates dataframes for force distributions, correction instances, images and additional data, based on a list of previously 
parsed data.

Args:    
    logger: Current script logging object.
    parsed_data: List of dictionaries, each containing the log line dynamic parsed data

Returns:
    List of dataframes for correction instances, force distributions, images and additional data, in that order.
"""


def generate_dataframes(
    logger: logging.Logger, parsed_data: list[dict[str, str]]
) -> list[pd.DataFrame]:
    global tracking_time

    ### Checkpoint - Start dataframes generation
    logger.info(
        "Start dataframes generation: {0}".format(str(time.time() - tracking_time))
    )
    tracking_time = time.time()

    dict_images = {
        "id_img": [],
        "exposition_start": [],
        "integration_time": [],
        "readout_start": [],
        "readout_stop": [],
        "ccd": [],
        "img_path": [],
    }

    dict_f_dist = {"id_f_dist": [], "forces": [], "timestamp": []}

    dict_additional_data = {
        "timestamp": [],
        "group": [],
        "label": [],
        "type": [],
        "value_str": [],
        "value_float": [],
        "value_int": [],
    }

    dict_corrections = {
        "timestamp": [],
        "id_f_dist_old": [],
        "id_f_dist_new": [],
        "id_img_old": [],
        "id_img_new": [],
    }

    # Parses a log line's date
    for line in parsed_data:

        if line["group"] == "FORCES":

            if line["label"] == "f_id":
                dict_f_dist["id_f_dist"].append(int(line["data"]))
                dict_f_dist["forces"].append([])
                dict_f_dist["timestamp"].append(
                    datetime.strptime(line["time"].split(".")[0], "%H:%M:%S").time()
                )

            elif line["label"] == "f_dist":
                f_dist_index = len(dict_f_dist["id_f_dist"]) - 1
                f_dist_content = line["data"].split()

                if f_dist_index >= 0:
                    for force in f_dist_content:
                        dict_f_dist["forces"][f_dist_index].append(float(force))

        elif line["group"] == "INIT":
            dict_corrections["timestamp"].append(
                datetime.strptime(line["time"].split(".")[0], "%H:%M:%S").time()
            )
            dict_corrections["id_f_dist_old"].append(None)
            dict_corrections["id_f_dist_new"].append(int(line["data"]))
            dict_corrections["id_img_old"].append(None)
            dict_corrections["id_img_new"].append(None)

        elif line["group"] == "IMAGE":

            if line["label"] == "INTTIME":
                dict_images["id_img"].append(None)
                dict_images["exposition_start"].append(
                    datetime.strptime(line["time"], "%H:%M:%S").time()
                )
                dict_images["integration_time"].append(int(line["data"]))
                dict_images["readout_start"].append(
                    datetime.strptime(line["time"], "%H:%M:%S").time()
                )
                dict_images["readout_stop"].append(
                    (
                        datetime.strptime(line["time"], "%H:%M:%S")
                        + timedelta(seconds=float(line["data"]))
                    ).time()
                )
                dict_images["ccd"].append(None)
                dict_images["img_path"].append(None)

            else:
                image_index = len(dict_images["id_img"]) - 1
                nu_img_flag = False

                if image_index == -1:
                    nu_img_flag = True

                else:
                    img_date = dict_images["exposition_start"][image_index]
                    buffer_min_day = datetime(1970, 1, 1)
                    acceptance_threshold = 2

                    if (
                        dict_images["integration_time"][image_index] is not None
                        and img_date
                        <= datetime.strptime(line["time"], "%H:%M:%S").time()
                        <= (
                            datetime.combine(buffer_min_day, img_date)
                            + timedelta(seconds=acceptance_threshold)
                        ).time()
                    ):
                        dict_images["id_img"][image_index] = int(line["data"])
                        dict_images["ccd"][image_index] = line["label"]

                    elif dict_images["integration_time"][image_index] is None:
                        nu_img_flag = True

                if nu_img_flag:
                    dict_images["id_img"].append(int(line["data"]))
                    dict_images["exposition_start"].append(
                        datetime.strptime(line["time"], "%H:%M:%S").time()
                    )
                    dict_images["integration_time"].append(None)
                    dict_images["readout_start"].append(
                        datetime.strptime(line["time"], "%H:%M:%S").time()
                    )
                    dict_images["readout_stop"].append(
                        (
                            datetime.strptime(line["time"], "%H:%M:%S")
                            + timedelta(seconds=float(line["data"]))
                        ).time()
                    )
                    dict_images["ccd"].append(line["label"])
                    dict_images["img_path"].append(None)

        else:
            dict_additional_data["timestamp"].append(
                datetime.strptime(line["time"], "%H:%M:%S").time()
            )
            dict_additional_data["group"].append(line["group"])
            dict_additional_data["label"].append(line["label"])

            value = line["data"]

            check_value = re.search(r"^-*[0-9]+[.0-9]*$", value)
            if check_value is not None:
                if "." in value:
                    dict_additional_data["type"].append("float")
                else:
                    dict_additional_data["type"].append("int")
            else:
                dict_additional_data["type"].append("str")

            try:
                dict_additional_data["value_str"].append(str(value))
            except:
                dict_additional_data["value_str"].append(" ")

            try:
                dict_additional_data["value_float"].append(float(value))
            except:
                dict_additional_data["value_float"].append(-9999999.0)

            try:
                dict_additional_data["value_int"].append(int(math.floor(value)))
            except:
                dict_additional_data["value_int"].append(-9999999)

    # Assign id column to dataframes
    df_f_dist = pd.DataFrame(dict_f_dist)

    df_additional_data = pd.DataFrame(dict_additional_data)
    df_additional_data["id_addt_data"] = df_additional_data.index

    df_corrections = pd.DataFrame(dict_corrections)
    df_corrections["id_corr"] = df_corrections.index

    df_images = pd.DataFrame(dict_images)
    df_images["id_image"] = df_images.index

    ### Checkpoint - End dataframes generation
    logger.info(
        "End dataframes generation: {0}".format(str(time.time() - tracking_time))
    )
    tracking_time = time.time()

    return [df_corrections, df_f_dist, df_images, df_additional_data]


"""
Builds the relationships between df_images and the images fits archives

Args:    
    logger: Current script logging object.
    df_images: Dataframe of images.

Returns:
    nu_df_images: Dataframe of images with corrected information
"""


def link_images(
    logger: logging.Logger, df_images: pd.DataFrame, img_folder: str
) -> pd.DataFrame:
    global tracking_time

    ### Checkpoint - Start link images
    logger.info("Start link images: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    for img_name in os.listdir(img_folder):
        with fits.open("{0}/{1}".format(img_folder, img_name)) as hdul:
            # Print information about the FITS file structure
            # hdul.info()

            # Access the primary HDU
            primary_hdu = hdul[0]

            # Get the header information
            header_info = primary_hdu.header

            # img_date = header_info['DATE-OBS']
            inttime = header_info["EXPTIME"]
            exp_no = header_info["HIERARCH ESO DET EXP NO"]

            try:
                index_img_list = df_images["id_img"].tolist().index(int(exp_no))

            except ValueError:
                # print('No match: {0}'.format(img_name))
                continue

            else:
                if df_images["integration_time"][index_img_list] == int(inttime):
                    df_images.loc[index_img_list, "img_path"] = img_name

                else:
                    print("Different inttime: {0}".format(img_name))

    ### Checkpoint - End link images
    logger.info("End link images: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    return df_images


"""
Validates the format of the image dataframe's rows, by sorting them by timestamp column and removing null ids.

Args:
    logger: Current script logging object.
    df_images: Dataframe of images.

Returns:
    Dataframe of valid images
"""


def validate_images(logger: logging.Logger, df_images: pd.DataFrame) -> pd.DataFrame:
    global tracking_time

    ### Checkpoint - Start validate images
    logger.info("Start validate images: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    nu_df_images = df_images.dropna(subset="id_img")

    nu_df_images = nu_df_images.sort_values(by="exposition_start")
    nu_df_images = nu_df_images.reset_index(drop=True)

    ### Checkpoint - End validate images
    logger.info("End validate images: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    # return pd.DataFrame(dict_corrections)
    return nu_df_images


"""
Validates the format of the force distribution dataframe's rows, by sorting them by timestamp column and removing null ids.
It also filters force distribution instances by filtering out those without inmediate previous and after images.

Args:    
    logger: Current script logging object.
    df_f_dist: Dataframe of force distributions.
    df_images: Dataframe of images.

Returns:
    Dataframe of valid force distributions
"""


def validate_forces(
    logger: logging.Logger, df_f_dist: pd.DataFrame, df_images: pd.DataFrame
) -> pd.DataFrame:
    global tracking_time

    ### Checkpoint - Start validate images
    logger.info("Start validate forces: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    nu_df_f_dist = df_f_dist.copy()
    img_times_buffer = df_images["exposition_start"].copy().tolist()
    f_dist_index = 0

    while f_dist_index < len(nu_df_f_dist["id_f_dist"]):
        remove_flag = True

        for img_times_index in range(len(img_times_buffer) - 1):
            curr_time = img_times_buffer[img_times_index]
            next_time = img_times_buffer[img_times_index + 1]

            if (
                curr_time < nu_df_f_dist["timestamp"][f_dist_index]
                and nu_df_f_dist["timestamp"][f_dist_index] < next_time
            ):
                remove_flag = False

                img_times_buffer.remove(curr_time)
                img_times_buffer.remove(next_time)

                break

        if remove_flag:
            f_dist_remove_index = nu_df_f_dist["id_f_dist"][f_dist_index]
            nu_df_f_dist = nu_df_f_dist[
                nu_df_f_dist["id_f_dist"] != f_dist_remove_index
            ]

        f_dist_index += 1

    nu_df_f_dist = nu_df_f_dist.sort_values(by="timestamp")
    nu_df_f_dist = nu_df_f_dist.reset_index(drop=True)

    ### Checkpoint - End validate images
    logger.info("End validate forces: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    # return pd.DataFrame(dict_corrections)
    return nu_df_f_dist


"""
Validates the format of the corrections dataframe's rows, by sorting them by timestamp column and removing null ids.
It also filters out the correction instances without inmediate following force re-distribution instances.

Args:
    logger: Current script logging object.
    df_corrections: Dataframe of timestamps of corrections.
    df_f_dist: Dataframe of force distributions.

Returns:
    Dataframe of valid corrections
"""


def validate_corrections(
    logger: logging.Logger, df_corrections: pd.DataFrame, df_f_dist: pd.DataFrame
):
    global tracking_time

    ### Checkpoint - Start validate corrections
    logger.info(
        "Start validate corrections: {0}".format(str(time.time() - tracking_time))
    )
    tracking_time = time.time()

    f_dist_id_list = df_f_dist["id_f_dist"].tolist()

    nu_df_corrections = df_corrections[
        df_corrections["id_f_dist_new"].isin(f_dist_id_list)
    ]

    nu_df_corrections = nu_df_corrections.sort_values(by="timestamp")
    nu_df_corrections = nu_df_corrections.reset_index(drop=True)

    ### Checkpoint - End validate correections
    logger.info(
        "End validate corrections: {0}".format(str(time.time() - tracking_time))
    )
    tracking_time = time.time()

    # return pd.DataFrame(dict_corrections)
    return nu_df_corrections


"""
Builds the relationships between df_corrections and an specified dataframe of related attributes.

Args:
    logger: Current script logging object.
    df_corrections: Dataframe of timestamps of corrections.
    df_attr: Dataframe of the specified attribute.
    name_attr: Name of the attribute to relate with the corrections.
    time_field: Name of the time column of the specified dataframe.

Returns:
    df_corrections: Dataframe of corrections' timestamps and related ids
"""


def link_dataframes(
    logger: logging.Logger,
    df_corrections: pd.DataFrame,
    df_attr: pd.DataFrame,
    name_attr: str,
    time_field: str,
) -> pd.DataFrame:
    global tracking_time

    ### Checkpoint - Start link dataframes
    logger.info("Start link dataframes: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    halfday = datetime.strptime("12:00:00", "%H:%M:%S").time()

    num_corrections = len(df_corrections["id_corr"])

    # New temporary column for better conditioning
    buffer_c_day = datetime(1970, 1, 1)

    try:
        df_attr["unix"] = df_attr[time_field].apply(
            lambda x: (
                datetime.combine(buffer_c_day, x).timestamp()
                if x >= halfday
                else datetime.combine(buffer_c_day + timedelta(days=1), x).timestamp()
            )
        )

        # Set a maximum boundary from the max time in df_attr's unix column
        upper_boundary = df_attr["unix"].iloc[-1]

        for index in range(num_corrections):

            # Time of current correction
            curr_time = df_corrections.loc[index, "timestamp"]

            # Link with attribute
            max_f_time = datetime.strptime("00:00:00", "%H:%M:%S").time()

            # Series indexes
            old_id = "id_{0}_old".format(name_attr)
            new_id = "id_{0}_new".format(name_attr)
            attr_id = "id_{0}".format(name_attr)

            # Add unix column for better conditioning
            buffer_day = datetime(1970, 1, 1)

            if curr_time < halfday:
                buffer_day += timedelta(days=1)

            curr_unix = datetime.combine(buffer_day, curr_time).timestamp()

            for f_time in df_attr["unix"]:

                if curr_unix > f_time and upper_boundary > f_time:
                    max_f_time = f_time

                else:
                    if max_f_time == datetime.strptime("00:00:00", "%H:%M:%S").time():
                        after_index = df_attr["unix"].tolist().index(f_time)

                        df_corrections.loc[index, old_id] = -1
                        df_corrections.loc[index, new_id] = df_attr.loc[
                            after_index, attr_id
                        ]

                    else:
                        before_index = df_attr["unix"].tolist().index(max_f_time)
                        after_index = df_attr["unix"].tolist().index(f_time)

                        df_corrections.loc[index, old_id] = df_attr.loc[
                            before_index, attr_id
                        ]
                        df_corrections.loc[index, new_id] = df_attr.loc[
                            after_index, attr_id
                        ]

                    break

        # Remove temporary column
        df_attr.drop("unix", axis=1, inplace=True)

    except IndexError:
        print("Dataframe for {0} is empty. No linking done.".format(name_attr))

    ### Checkpoint - End link dataframes
    logger.info("End link dataframes: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    # return pd.DataFrame(dict_corrections)
    return df_corrections


"""
Saves parsed data in an external file

Args:
    parsed_data: List of dictionaries, each containing the log line dynamic parsed data
    dest_arch_name: Relative path of file for parsed data

Returns:
    None
"""


def save_parsed_data(parsed_data, dest_arch_name) -> None:
    with open(dest_arch_name, "w") as data_file:
        json.dump(parsed_data, data_file)


"""
Generates and stores report of the analyzed lines.

Args:
    log_lines: List of the original log lines.
    mid_files_list: List of lists, each one with the results of intermediate algorithm steps.
    file_name: Name of the report file.

Returns:
    None
"""


def save_report(
    log_lines: list[str], mid_files_list: list[list[any]], file_name: str
) -> None:
    # Denormalizing lists
    _, _, parsed_lines = mid_files_list

    num_log_lines = len(log_lines)

    num_parsed_lines = len(parsed_lines)

    num_corr_lines = 0
    num_img_lines = 0
    num_forces_lines = 0

    for line in parsed_lines:
        if line["group"] == "INIT":
            num_corr_lines += 1

        elif line["group"] == "FORCES":
            num_forces_lines += 1

        elif line["group"] == "IMAGE":
            num_img_lines += 1

    num_deleted_lines = num_log_lines - num_parsed_lines

    with open(file_name, "w") as reporte:
        reporte.write("Number of log lines read: " + str(num_log_lines) + "\n")
        reporte.write("Number of log lines removed: " + str(num_deleted_lines))
        reporte.write("Number of log lines parsed: " + str(num_parsed_lines) + "\n")
        reporte.write(
            "Number of AO correction instance log lines parsed: "
            + str(num_corr_lines)
            + "\n"
        )
        reporte.write(
            "Number of force re-distribution instance log lines parsed: "
            + str(num_forces_lines)
            + "\n"
        )
        reporte.write(
            "Number of image obtention instance log lines parsed: "
            + str(num_img_lines)
            + "\n"
        )


"""
Saves parsed data in an external CSV file

Args:
    df_list: List of the final dataframes.
    folder: Name or path of the folder to store the CSV files.

Returns:
    None
"""


def save_df_as_csv(df_list: list[pd.DataFrame], folder: str) -> None:
    df_corrections, df_f_dist, df_images, df_additional_data = df_list

    #### Save df in csv files
    df_f_dist.to_csv("{0}/df_f_dist.csv".format(folder), index=False)
    df_additional_data.to_csv("{0}/df_additional_data.csv".format(folder), index=False)
    df_corrections.to_csv("{0}/df_corrections.csv".format(folder), index=False)
    df_images.to_csv("{0}/df_images.csv".format(folder), index=False)


"""
Print the dataframes on console.

Args:
    df_list: List of the final dataframes.
Returns:
    None
"""


def print_df_console(df_list: list[pd.DataFrame]) -> None:
    df_corrections, df_f_dist, df_images, df_additional_data = df_list

    #### Save df in csv files
    print(df_f_dist)
    print(df_additional_data)
    print(df_corrections)
    print(df_images)


"""
Parses arguments and optional flags for console execution.

Args:
    args_list: System console arguments.

Returns:
    Namespace with the defined arguments and optional flags.
"""


def parse_args(args_list: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("date", type=str, help="Date to analyzed")

    parser.add_argument(
        "ut", type=str, help="Number of the UT telescope to be analyzed"
    )

    parser.add_argument(
        "-t",
        "--templatefile",
        type=str,
        help="Path to the text file where the templates are stored (if no name is written, ./poc_templates.txt will be set by default)",
    )

    parser.add_argument(
        "-p",
        "--preprocess",
        action="store_false",
        help="Skips logs pre-processing stage: removal of log headers and grouping of log lines in sections, based on when AG GUIDE has stopped activity",
    )

    parser.add_argument(
        "-o",
        "--obsprocess",
        action="store_false",
        help="Skips observation files processing stage: filtering of log lines based on time intervals where a valid observation has been performed",
    )

    parser.add_argument(
        "-i",
        "--linkimages",
        action="store_false",
        help="Skips linking of image log data with fits files: write of the fits files' path respective to each image row in the images' dataframe",
    )

    parser.add_argument(
        "-l",
        "--linkdataframes",
        action="store_false",
        help="Skips linking of correction dataframe with related dataframes: write of the ids of the force distribution and image respective to each row in the correction dataframe",
    )

    parser.add_argument(
        "-c",
        "--console",
        action="store_true",
        help="Prints resulting dataframes on console",
    )

    parser.add_argument(
        "-s", "--save", type=str, help="Stores resulting dataframes in csv files"
    )

    parser.add_argument(
        "-r",
        "--report",
        action="store_true",
        help="Prints a report detailing the processing of log lines",
    )

    args = parser.parse_args(args_list)

    return args


"""
Order of execution of the algorithm major stages.

Args:
    logger: Current script logging object.
    args: Namespace with the defined arguments and optional flags.

Returns:
    None
"""


def process_path(logger: logging.Logger, args: argparse.Namespace) -> None:
    #### Original log lines
    log_lines = open_txt_file("logs/wt{0}tcs.{1}.log".format(args.ut, args.date))

    #### Subpath that generates the dataframes
    df_list, mid_list = dataframe_generation_subpath(
        logger=logger,
        log_lines=log_lines,
        date=args.date,
        tplt_arch_flag=args.templatefile,
        pre_process_flag=args.preprocess,
        obs_process_flag=args.obsprocess,
    )

    #### Subpath that refines and stores the dataframes
    refined_df_list = dataframe_refining_subpath(
        logger=logger,
        df_list=df_list,
        img_linking=args.linkimages,
        df_linking=args.linkdataframes,
    )

    dataframe_showcasing_subpath(
        logger=logger,
        log_file=log_lines,
        mid_files_list=mid_list,
        df_list=refined_df_list,
        console_flag=args.console,
        save_flag=args.save,
        report_flag=args.report,
    )


"""
Invokes the algorithm methods to pre-process the log lines, filter them by valid observations, parsed them and generate the 
respective dataframes.

Args:
    logger: Current script logging object.
    log_lines: List of the original log lines.
    date: Date, in string format, of the night of the log file.
    tplt_arch_flag: Flag, and name if true, for the use of a customized template filename.
    pre_process_flag: Flag for the use of a pre-processing stage.
    obs_process_flag: Flag for the use of an observation filtering stage.

Returns:
    List of lists, the first of generated dataframes and the second of the intermediate algorithm stages.
"""


def dataframe_generation_subpath(
    logger: logging.Logger,
    log_lines: list[str],
    date: str,
    tplt_arch_flag: str,
    pre_process_flag: bool,
    obs_process_flag: bool,
) -> list[list[any]]:
    #### Log lines pre-processing
    pre_processed_logs = None

    if pre_process_flag:
        pre_processed_logs = log_formatting(logger, log_lines)

        # Save pre-processed logs to file
        with open("mid_files/pre_processed_logs.txt", "w", encoding="utf-8") as f:
            for log_line in pre_processed_logs:
                f.write("\t" + log_line)

    else:
        pre_processed_logs = log_lines

        # Save pre-processed logs to file
        with open("mid_files/pre_processed_logs.txt", "w", encoding="utf-8") as f:
            for log_line in pre_processed_logs:
                f.write("\t" + log_line)

    #### Log lines' observation filtering
    obs_logs = pre_processed_logs

    if obs_process_flag:
        fetch_obs_file("obs_files", date)
        obs_list = open_obs_file("obs_files/{0}.csv".format(date))
        obs_logs = new_obs_filtering(logger, pre_processed_logs, obs_list, date)

        # Save observation logs to file
        with open("mid_files/observation_logs.txt", "w") as f:
            for obs_section_key in obs_logs.keys().__iter__():
                f.write("\t" + obs_section_key + "\t")
                for obs_line in obs_logs[obs_section_key]:
                    f.write("\t\t" + obs_line)

    #### Log lines parsing

    log_parsing = True
    parsed_data = None
    tplt_arch_name = "poc_templates"

    if tplt_arch_flag:
        tplt_arch_name = tplt_arch_flag

    if log_parsing:
        tplt_list = open_txt_file("{0}.txt".format(tplt_arch_name))
        parsed_data = log_parsing_regex(logger, obs_logs, tplt_list)

        # Save parsed data to file
        with open("mid_files/parsed_data.txt", "w") as f:
            for data_dict in parsed_data:
                parsed_values = json.dumps(data_dict)
                f.writelines(parsed_values + "\n")
    else:
        parsed_data = []

        # Load from file
        with open("mid_files/parsed_data.txt") as f:
            for data_dict in f.readlines():
                print(data_dict.split("\t"))
                parsed_data.append(data_dict.split("\t"))

    #### Parsed data classifier
    df_list = generate_dataframes(logger, parsed_data)

    #### Mid files list
    mid_files_list = [pre_processed_logs, obs_logs, parsed_data]

    return [df_list, mid_files_list]


"""
Invokes the algorithm methods to validate the format of the dataframes for images, force distributions and correction instances,
linkes the image dataframe with the respective fits files, and links the correction dataframe with the respective force distribution
and image for each row. 
respective dataframes.

Args:
    logger: Current script logging object.
    df_list: List of the previously generated dataframes.
    img_linking: Flag for the linking of the image dataframe with the respective fits file for each row.
    df_linking: Flag for the linking of the correction dataframe with the respective force distribution and image for each row.

Returns:
    List of the refined dataframes.
"""


def dataframe_refining_subpath(
    logger: logging.Logger,
    df_list: list[pd.DataFrame],
    img_linking: bool,
    df_linking: bool,
) -> list[pd.DataFrame]:
    #### Denormalizing dataframe list
    df_corrections, df_f_dist, df_images, df_additional_data = df_list

    #### Validate successful images instances
    df_images = validate_images(logger, df_images)

    #### Validate successful force distribution instances
    df_f_dist = validate_forces(logger, df_f_dist, df_images)

    #### Linking df_images and fits files
    if img_linking:
        df_images = link_images(logger, df_images, "img_files")

    #### Validate successful correction instances
    df_corrections = validate_corrections(logger, df_corrections, df_f_dist)

    #### Link forces distributions and images with correction instances
    if df_linking:
        df_corrections = link_dataframes(
            logger, df_corrections, df_f_dist, "f_dist", "timestamp"
        )
        df_corrections = link_dataframes(
            logger, df_corrections, df_images, "img", "exposition_start"
        )

    #### Refined dataframe list
    nu_df_list = [df_corrections, df_f_dist, df_images, df_additional_data]

    return nu_df_list


"""
Invokes the algorithm methods to save the generated dataframes as a csv file, print on console or generate a report file to showcase
the algorithm performance.

Args:
    logger: Current script logging object.
    log_file: List of the original log files.
    mid_files_list: List of the intermediate algorithm stages results.
    df_list: List of the previously generated dataframes.
    console_flag: Flag for the use of the console stage.
    save_flag: Flag for the use of the save as csv stage.
    report_flag: Flag for the use of the report file stage.

Returns:
    None
"""


def dataframe_showcasing_subpath(
    logger: logging.Logger,
    log_file: list[str],
    mid_files_list: list[list[str]],
    df_list: list[pd.DataFrame],
    console_flag: bool,
    save_flag: str,
    report_flag: str,
) -> None:
    #### Denormalizing dataframe list
    #### Save dataframes as csv
    if save_flag:
        save_df_as_csv(df_list, save_flag)

    #### Print dataframes in console
    if console_flag:
        print_df_console(df_list)

    #### Print dataframes in console
    if report_flag:
        save_report(
            log_file,
            mid_files_list,
            "Reporte {0}".format(datetime.now().strftime("%Y-%m-%d_%H-%M-%S")),
        )


#### Main
def main(argv: list[str]):
    #### MVP Logger
    logging.basicConfig(filename="poc.log", level=logging.INFO, filemode="w")
    logger = logging.getLogger("myLogger")

    #### Timer
    init_time = time.time()

    #### Arg parser
    args = parse_args(argv)

    #### Processing path
    process_path(logger, args)

    ### Final time
    print("Final time: ", str(time.time() - init_time))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
