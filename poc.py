from astropy.io import fits
from datetime import datetime, timedelta
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
from io import StringIO
from email.mime.multipart import MIMEMultipart
#from memory_profiler import profile
#from ttp import ttp


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
    A list of tuples, with each tuple containing the observation ID and it's respective timestamp.
"""
def open_obs_file(arch_name):
    tpl_list = []

    with open(arch_name, mode="r") as query:
        reader = csv.reader(query)
        
        next(reader)

        for line in reader:
            if(line[11] != ''):
                tpl_list.append((line[10], line[11].split("T")[1]))

    tpl_list.pop(0)

    return tpl_list

"""
Opens a text file and deposits it's information into a list.

Args:
    arch_name: Text file relative path.

Returns:
    A list with all the lines as strings.
"""
def open_txt_file(arch_name):
    with open(file=arch_name, mode='r', encoding='iso-8859-1') as logs:
        lines = logs.readlines()

    return lines


"""
Fetch the VLT Observations CSV file from the ESO raw database and saves it in the obs_files folder
Args:
    arch_name: CSV file relative path.

Returns:
    A list of tuples, with each tuple containing the observation ID and it's respective timestamp.
"""
def fetch_obs_file(destination_folder, obs_date):
    url = 'https://archive.eso.org/wdb/wdb/eso/eso_archive_main/query'

    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0) Gecko/20100101 Firefox/142.0",
        "Referer": "https://archive.eso.org/eso/eso_archive_main.html"
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
        "spectrum[]": (None, "FORS2;'SPECTRUM%','ECHELLE%','MOS%','MXU%','HIT%','IMAGE_SPECTRUM%'"),
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
        "add": (None, "((ins_id like 'EFOSC%' AND (dp_tech like 'IMA%')) or (ins_id like 'EMMI%' AND (dp_tech like 'IMA%')) or (ins_id like 'ERIS%') or (ins_id like ('ERIS%') AND ((dp_tech like 'IMA%') AND (dp_tech not like '%SAM%'))) or (ins_id like 'FORS1%' AND (dp_tech like 'IMA%')) or (ins_id like 'FORS2%' AND (dp_tech like 'IMA%')) or (ins_id like 'HAWKI%' AND (dp_tech like 'IMA%')) or (ins_id like 'GROND%') or (ins_id like 'ISAAC%' AND (dp_tech like 'IMA%')) or (ins_id like 'NAOS+CONICA%' AND (dp_tech like 'IMA%' OR dp_tech like 'SDI%')) or (ins_id like 'OMEGACAM%') or (ins_id like 'SOFI%' AND (dp_tech like 'IMA%')) or (ins_id like 'SPHERE%' AND (dp_tech like 'IMA%')) or (ins_id like 'SUSI%') or (ins_id like 'TIMMI%' AND (dp_tech like 'IMA%')) or (ins_id like 'VIMOS%' AND (dp_tech like 'IMA%')) or (ins_id like 'VIRCAM%') or (ins_id like 'VISIR%' AND (dp_tech like 'IMA%')) or (ins_id like 'WFI%') or (ins_id like 'XSHOOTER%' AND (dp_tech like 'IMA%')) or (ins_id like 'CES%') or (ins_id like 'CRIRE%' AND ((dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'EFOSC%' AND ((dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'EMMI%' AND ((dp_tech like 'SPECTRUM%' OR dp_tech like 'ECHELLE%' OR dp_tech like 'MOS%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'ERIS%' AND ((dp_tech like 'IFU%' OR dp_tech like '%LSS%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'ESPRESSO%') or (ins_id like 'FEROS%') or (ins_id like 'FORS1%' AND ((dp_tech like 'SPECTRUM%' OR dp_tech like 'MOS%' OR dp_tech like 'IMAGE_SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'FORS2%' AND ((dp_tech like 'SPECTRUM%' OR dp_tech like 'ECHELLE%' OR dp_tech like 'MOS%' OR dp_tech like 'MXU%' OR dp_tech like 'HIT%' OR dp_tech like 'IMAGE_SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'GIRAF%') or (ins_id like 'HARPS%') or (ins_id like 'ISAAC%' AND ((dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'KMOS%') or (ins_id like 'MUSE%') or (ins_id like 'NAOS+CONICA%' AND ((dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'NIRPS%') or (ins_id like 'SINFO%') or (ins_id like 'SOFI%' AND ((dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'SPHERE%' AND ((dp_tech like 'IFU%' OR dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'TIMMI%' AND ((dp_tech like 'SPECTRUM%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'UVES%') or (ins_id like 'VIMOS%' AND ((dp_tech like 'IFU%' OR dp_tech like 'MOS%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'VISIR%' AND ((dp_tech like 'SPECTRUM%' OR dp_tech like 'ECHELLE%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'SHOOT%') or (ins_id in ('SHOOT','XSHOOTER') AND ((dp_tech like 'ECHELLE%') OR (dp_cat != 'SCIENCE'))) or (ins_id like 'AMBER%') or (ins_id like 'GRAVITY%') or (ins_id like 'MATISSE%') or (ins_id like 'MIDI%') or (ins_id like 'PIONIER%') or (ins_id like 'VINCI%') or (ins_id like 'EFOSC%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'FORS1%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'FORS2%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'ISAAC%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'NAOS+CONICA%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'SOFI%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'SPHERE%' AND (dp_tech like 'POLARIM%')) or (ins_id like 'EFOSC%' AND (dp_tech like '%CORO%')) or (ins_id like 'ERIS%' AND (dp_tech like 'CORO%')) or (ins_id like 'NAOS+CONICA%' AND (dp_tech like '%CORO%')) or (ins_id like 'SPHERE%' AND (dp_tech like '%CORO%')) or (ins_id like 'VISIR%' AND (dp_tech like '%CORO%')) or (ins_id like 'ALPACA%') or (ins_id like 'APICAM%') or (ins_id like 'APEXBOL%') or (ins_id like 'APEXHET%') or (ins_id like 'FAIM6%') or (ins_id like 'FAIM7%') or (ins_id like 'GRIPS19%') or (ins_id like 'LGSF%') or (ins_id like 'MAD%') or (ins_id like 'MASCOT%') or (ins_id like 'SPECU%') or (ins_id like 'WFCAM%') or (ins_id like 'ERIS%' AND (dp_tech like '%SAM%')) or (ins_id like 'NAOS+CONICA%' AND (dp_tech like '%SAM%')) or (ins_id like 'SPHERE%' AND (dp_tech like '%SAM%')) or (ins_id like 'VISIR%' AND (dp_tech like 'SAM%')))"),
        "tab_tel_airm_start": (None, "on"),
        "tab_stat_instrument": (None, "on"),
        "tab_ambient": (None, "on"),
        "tab_stat_exptime": (None, "on"),
        "tab_HDR": (None, "on"),
        "tab_mjd_obs": (None, "on"),
        "aladin_colour": (None, "aladin_instrument"),
        "tab_stat_plot": (None, "on"),
        "order": (None, "")
    }

    # Make a POST request with data
    response = requests.post(url, files=payload, headers=header)

    # Access the response body as CSV
    if response.status_code == 200:
        obs_csv_list = []

        csv_data = StringIO(response.text)
        reader = csv.reader(csv_data)

        for row in reader:
            if(len(row) > 1):
                obs_csv_list.append(row)

        with open("{0}/{1}.csv".format(destination_folder, obs_date), 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)

            # Write the header row
            writer.writerow(obs_csv_list[0])

            # Write the remaining data rows
            writer.writerows(obs_csv_list[1:])
        
    else:
        print("Error while fetching observation csv ", response.status_code)

    return response.status_code


"""
Removes the header of each log line and groups them in blocks, according the timeframe between the VLT autoguider stopping an iteration
and the beggining of the following iteration, which corresponds to an observation period.

Args:
    logger: Current script logging object.
    log_lines: List of log lines, in string form.

Returns:
    A list of lists, with each containing a group of tuples, with the id of an observation first and 
    the pre-processed log line last, fitted by observation period.
"""
def log_pre_processing(logger: logging.Logger, log_lines: list[str]):
    global tracking_time

    ### Checkpoint - Start log pre-processing
    logger.info('Start log pre-processing: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()
    
    headerless_lines = []
    extraction_flag = False

    for index, line in enumerate(log_lines):
        
        # Log line header removal
        header_re = re.compile(r'([a-zA-Z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}?)\swt[1-4]tcs\s(.+)(\[[0-9]+\]):\s')

        headerless_line = re.sub(header_re, '', line)

        headerless_line = headerless_line.replace("  ", " ")
        headerless_lines.append((index, headerless_line))

        ### Checkpoint - Line added to current list
        logger.info('Line added to current list: {0}'.format(str(time.time() - tracking_time)))
        tracking_time = time.time()

        #if(headerless_line.find("AG.GUIDE") != -1):
            ### Checkpoint - AG.GUIDE line found
            #logger.info('AG.GUIDE line found: {0}'.format(str(time.time() - tracking_time)))
            #tracking_time = time.time()

            #if(headerless_line.find("STOP") != -1):
                # Start log line extraction
                #headerless_lines.append([])
                #extraction_flag = True

            #elif(headerless_line.find("START") != -1):
                # Finish log line extraction
                #if(len(headerless_lines) != 0):
                    #headerless_lines[-1].append((index, headerless_line))

                #extraction_flag = False

        #if(extraction_flag): 
            #headerless_line = headerless_line.replace("  ", " ")
            #headerless_lines[-1].append((index, headerless_line))

            ### Checkpoint - Line added to current list
            #logger.info('Line added to current list: {0}'.format(str(time.time() - tracking_time)))
            #tracking_time = time.time()
    
    ### Checkpoint - End log pre-processing
    logger.info('End log pre-processing: {0}'.format(str(time.time() - tracking_time)))
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
def obs_filtering(logger: logging.Logger, log_lines: list[list[str]], obs_list: list[str, str]):
    global tracking_time

    ### Checkpoint - Start observation log lines filtering
    logger.info('Start observation log lines filtering: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()
            
    obs_log_sections = []

    #Check which log line groups match their timeframes with an observation timestamp
    for log_section in log_lines:

        stop_line = log_section[0][1]
        start_line = log_section[-1][1]
        
        time_re = re.compile(r'[0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]+)*')

        stop_time = re.search(time_re,stop_line).group()
        start_time = re.search(time_re,start_line).group()

        for tpl_id, tpl_time in obs_list:

            if(stop_time < tpl_time and tpl_time < start_time):
                ### Checkpoint - Line within successful observation timeframe found
                logger.info('Line within successful observation timeframe found: {0}'.format(str(time.time() - tracking_time)))
                tracking_time = time.time()

                #Save log line group
                obs_log_sections.append(log_section)
                break

    ### Checkpoint - End observation log lines filtering
    logger.info('End observation log lines filtering: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    return obs_log_sections


"""
Parsess each log line and extracts their dynamic data, using the Text Template Parsing library (ttp).

Args:
    logger: Current script logging object.
    log_sections: List of lists of log lines, grouped by successful observation period, in string form.
    templates_list: List of log line parsing templates, in string form.

Returns:
    A list of dictionaries, with each containing the dynamic data extracted from a log line.
"""
def log_parsing_ttp(logger: logging.Logger, log_sections: list[list[str]], templates_list: list[str]):
    global tracking_time

    ### Checkpoint - Start log line parsing
    logger.info('Start log line parsing: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    parsed_data = []

    for log_section in log_sections:
        for index, line in log_section:

            #Iterates through all templates
            for template in templates_list:

                #Log line parsing
                try:
                    # Parsing with Text Template Parsing library
                    parser = ttp(data=line, template=template)
                    parser.parse()
                    key = list(parser.result(template='per_input', structure="dictionary"))[0]
                    result = parser.result(template='per_input', structure="dictionary")[key][0]

                    # If match is true, the data extracted is saved and jumps into the next log line
                    if(len(result) != 0):
                        # Save parsing result
                        result['Num_linea'] = index
                        parsed_data.append(result)
                        break

                    ### Checkpoint - Log line parsing attempt
                    logger.info('Log line parsing attempt: {0}'.format(str(time.time() - tracking_time)))
                    tracking_time = time.time()

                #Recolecta otros errores sobre la plantilla usada (se revisarán a posteriori)
                except xml.etree.ElementTree.ParseError as err:
                    logger.info("ERROR: " + str(err))

            ### Checkpoint - End iteration on template list
            logger.info('End iteration on template list: {0}'.format(str(time.time() - tracking_time)))
            tracking_time = time.time()

        ### Checkpoint - End iteration on log line group 
        logger.info('End iteration on log line group: {0}'.format(str(time.time() - tracking_time)))
        tracking_time = time.time()

    ### Checkpoint - End log line parsing
    logger.info('End log line parsing: {0}'.format(str(time.time() - tracking_time)))
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
def log_parsing_regex(logger: logging.Logger, log_sections: list[str, str], templates_list: list[str]):
    global tracking_time

    ### Checkpoint - Start log line parsing
    logger.info('Start log line parsing: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    parsed_data = []

    #for log_section in log_sections:    
    for index, line in log_sections: 
        result = {}
            
        #Iterates through all templates 
        for template in templates_list: 
            template = template.replace("\n", "")
            #Parsing with regular expressions
            parser = re.search(template, line)                

            # If match is true, the data extracted is saved and jumps into the next log line
            if(parser is not None):

            #check_line_f_dist = re.search(r"SetGlbAbs", parser.group())
            #if(check_line_f_dist is not None):
                #    print(parser.group())

                # Parses a log line's date
                check_forces = re.search(r"SetGlbAbs", parser.group())
                check_inttime = re.search(r"INTTIME", parser.group())
                check_exp_no= re.search(r"EXP NO", parser.group())
                    

                if(check_forces is not None):

                    check_f_dist = re.search(r"Forces", parser.group())
                    check_f_header = re.search(r"Executed", parser.group())
                        
                    if(check_f_dist is not None):
                        result["group"] = "forces"
                        result["label"] = "f_dist"
                        result["date"] = parser.group(2)
                        result["time"] = parser.group(3)
                        result["data"] = parser.group(6)

                    elif(check_f_header is not None):
                        result["group"] = "forces"
                        result["label"] = "f_id"
                        result["date"] = parser.group(2)
                        result["time"] = parser.group(3)
                        result["data"] = parser.group(5)

                    else:
                        result["time"] = parser.group(3)
                        result["data"] = parser.group(5)
                        result["group"] = "INIT"

                elif(check_exp_no is not None):
                    result["time"] = parser.group(2)
                    result["group"] = "IMAGE"
                    result["label"] = parser.group(5)
                    result["data"] = parser.group(4)

                elif(check_inttime is not None):
                    result["time"] = parser.group(2)
                    result["group"] = "IMAGE"
                    result["label"] = parser.group(4)
                    result["data"] = parser.group(5)

                else:
                    result["time"] = parser.group(2)
                    result["group"] = parser.group(4)
                    result["label"] = parser.group(5)
                    result["data"] = parser.group(6)

                # Save parsing result
                result['Num_linea'] = index
                parsed_data.append(result)
                break

            ### Checkpoint - Log line parsing attempt
            logger.info('Log line parsing attempt: {0}'.format(str(time.time() - tracking_time)))
            tracking_time = time.time()


        ### Checkpoint - End iteration on template list
        logger.info('End iteration on template list: {0}'.format(str(time.time() - tracking_time)))
        tracking_time = time.time()

        ### Checkpoint - End iteration on log line group 
        #logger.info('End iteration on log line group: {0}'.format(str(time.time() - tracking_time)))
        #tracking_time = time.time()

    ### Checkpoint - End log line parsing
    logger.info('End log line parsing: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    return parsed_data

"""
Classifies the parsed log lines

Args:
    parsed_data: List of dictionaries, each containing the log line dynamic parsed data

Returns:
    None
"""
def generate_dataframes(logger: logging.Logger, parsed_data: list[dict[str, str]]):
    global tracking_time

    ### Checkpoint - Start dataframes generation
    logger.info('Start dataframes generation: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    dict_images = {
        "id_img": [],
        "exposition_start": [],
        "integration_time": [],
        "readout_start": [],
        "readout_stop": [],
        "ccd": [],
        "img_path": []
    }
    
    dict_f_dist = {
        "id_f_dist": [],
        "forces": [],
        "timestamp": []
    }

    dict_additional_data = {
        "timestamp": [],
        "group": [],
        "label": [],
        "type": [],
        "value_str": [],
        "value_float": [],
        "value_int": []
    }

    dict_corrections = {
        "timestamp": [],
        "id_f_dist_old": [],
        "id_f_dist_new": [],
        "id_img_old": [],
        "id_img_new": []
    }
    
    # Parses a log line's date
    for line in parsed_data:

        if(line["group"] == "forces"):

            if(line["label"] == "f_id"):
                dict_f_dist["id_f_dist"].append(int(line["data"]))
                dict_f_dist["forces"].append([])
                dict_f_dist["timestamp"].append(datetime.strptime(line["time"].split(".")[0], "%H:%M:%S").time())

            elif(line["label"] == "f_dist"):
                f_dist_index = len(dict_f_dist["id_f_dist"]) - 1
                f_dist_content = line["data"].split()

                if(f_dist_index >= 0):
                    for force in f_dist_content:
                        dict_f_dist["forces"][f_dist_index].append(float(force))

        elif(line["group"] == "INIT"):
            dict_corrections["timestamp"].append(datetime.strptime(line["time"].split(".")[0], "%H:%M:%S").time())
            dict_corrections["id_f_dist_old"].append(None)
            dict_corrections["id_f_dist_new"].append(int(line["data"]))
            dict_corrections["id_img_old"].append(None)
            dict_corrections["id_img_new"].append(None)

        elif(line["group"] == "IMAGE"):

            if(line["label"] == "INTTIME"):
                dict_images["id_img"].append(None)
                dict_images["exposition_start"].append(datetime.strptime(line["time"], "%H:%M:%S").time())
                dict_images["integration_time"].append(line["data"])
                dict_images["readout_start"].append(datetime.strptime(line["time"], "%H:%M:%S").time())
                dict_images["readout_stop"].append((datetime.strptime(line["time"], "%H:%M:%S") + timedelta(seconds=float(line["data"]))).time())
                dict_images["ccd"].append(None)
                dict_images["img_path"].append(None)

            else:
                image_index = len(dict_images["id_img"]) - 1

                if(image_index != -1):
                    if (dict_images["exposition_start"][image_index] == datetime.strptime(line["time"], "%H:%M:%S").time()):
                        dict_images["id_img"][image_index] = line["data"]
                        dict_images["ccd"][image_index] = line["label"]

        else:
            dict_additional_data["timestamp"].append(datetime.strptime(line["time"], "%H:%M:%S").time())
            dict_additional_data["group"].append(line["group"])
            dict_additional_data["label"].append(line["label"])

            value = line["data"]

            check_value = re.search(r"^-*[0-9]+[.0-9]*$", value)
            if(check_value is not None):
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
    logger.info('End dataframes generation: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    return [df_f_dist, df_additional_data, df_corrections, df_images]


"""
Builds the relationships between df_images and the images fit archives

Args:
    df_images: Dataframe of images

Returns:
    nu_df_images: Dataframe of images with corrected information
"""
def link_images(logger: logging.Logger, df_images: pd.DataFrame, img_folder: str):
    global tracking_time
    
    ### Checkpoint - Start link images
    logger.info('Start link images: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    for img_path in img_folder:
        with fits.open("img_files/{0}".format(img_path)) as hdul:
            # Print information about the FITS file structure
            hdul.info()

            # Access the primary HDU
            primary_hdu = hdul[0]

            # Get the header information
            header_info = primary_hdu.header

            #img_date = header_info['DATE-OBS']
            inttime = header_info['EXPTIME']
            exp_no = header_info['HIERARCH ESO DET EXP NO']

            try:
                index_img_list = df_images['id_img'].index(exp_no)

            except ValueError:
                continue

            else:
                if(df_images['integration_time'][index_img_list] == inttime):
                    df_images['img_path'][index_img_list] = img_path

    ### Checkpoint - End link images
    logger.info('End link images: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    return df_images


"""
Builds the relationships between df_corrections and the rest of the dataframes

Args:
    df_corrections: Dataframe of timestamps of corrections
    df_f_dist: Dataframe of force distributions

Returns:
    df_corrections: Dataframe of corrections' timestamps and related ids
"""
def validate_corrections(logger: logging.Logger, df_corrections: pd.DataFrame, df_f_dist: pd.DataFrame):
    global tracking_time
    
    ### Checkpoint - Start validate corrections
    logger.info('Start validate corrections: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    f_dist_id_list = df_f_dist["id_f_dist"].tolist()
    
    nu_df_corrections = df_corrections[df_corrections["id_f_dist_new"].isin(f_dist_id_list)]

    nu_df_corrections = nu_df_corrections.reset_index(drop=True)

    ### Checkpoint - End validate correections
    logger.info('End validate corrections: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    #return pd.DataFrame(dict_corrections)
    return nu_df_corrections


"""
Builds the relationships between df_corrections and the rest of the dataframes

Args:
    df_corrections: Dataframe of timestamps of corrections
    df_f_dist: Dataframe of force distributions

Returns:
    df_corrections: Dataframe of corrections' timestamps and related ids
"""
def link_dataframes(logger: logging.Logger, df_corrections: pd.DataFrame, df_attr: pd.DataFrame, name_attr: str, time_field: str):
    global tracking_time
    
    ### Checkpoint - Start link dataframes
    logger.info('Start link dataframes: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    halfday = datetime.strptime("12:00:00", "%H:%M:%S").time()

    num_corrections = len(df_corrections["id_corr"])

    # New temporary column for better conditioning
    buffer_c_day = datetime(1970, 1, 1)

    df_attr["unix"] = df_attr[time_field].apply(lambda x: datetime.combine(buffer_c_day, x).timestamp() 
            if x >= halfday 
            else datetime.combine(buffer_c_day + timedelta(days=1), x).timestamp())

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

        if(curr_time < halfday):
            buffer_day += timedelta(days=1)
           
        curr_unix = datetime.combine(buffer_day, curr_time).timestamp()
        print(curr_unix)

        for f_time in df_attr["unix"]:

            if(curr_unix > f_time):
                max_f_time = f_time

            else:
                if(max_f_time == datetime.strptime("00:00:00", "%H:%M:%S").time()):
                    after_index = df_attr["unix"].tolist().index(f_time)
                
                    df_corrections.loc[index, old_id] = -1
                    df_corrections.loc[index, new_id] = df_attr.loc[after_index, attr_id]
                
                else:
                    before_index = df_attr["unix"].tolist().index(max_f_time)
                    after_index = df_attr["unix"].tolist().index(f_time)

                    df_corrections.loc[index, old_id] = df_attr.loc[before_index, attr_id]
                    df_corrections.loc[index, new_id] = df_attr.loc[after_index, attr_id]

                break

    # Remove temporary column
    df_attr.drop("unix", axis=1, inplace=True)

    ### Checkpoint - End link dataframes
    logger.info('End link dataframes: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    #return pd.DataFrame(dict_corrections)
    return df_corrections


"""
Saves parsed data in an external file

Args:
    parsed_data: List of dictionaries, each containing the log line dynamic parsed data
    dest_arch_name: Relative path of file for parsed data

Returns:
    None
"""
def save_parsed_data(parsed_data, dest_arch_name):
    with open(dest_arch_name, "w") as data_file:
        json.dump(parsed_data, data_file)


#Main
if __name__ == '__main__':

    #### MVP Logger
    logging.basicConfig(filename='poc.log', level=logging.INFO)
    logger = logging.getLogger('myLogger')

    ### Import params
    with open('params.json', 'r') as file:
        params = json.load(file)

    ### Log date
    date = params["date"]
    ### UT number
    ut = params["ut"]
    ### Relative path of destination file for parsed data
    dest_arch_name = params["destination_filename"]
    ### Relative path of template file
    tplt_arch_name = params["template_filename"]

    #### Log lines pre-processing  
    pre_processed_logs_procesing = True
    pre_processed_logs = None

    if pre_processed_logs_procesing:
        log_lines = open_txt_file('logs/wt{0}tcs.{1}.log'.format(ut, date))
        pre_processed_logs = log_pre_processing(logger, log_lines)

        # Save pre-processed logs to file
        with open('mid_files/pre_processed_logs.txt', "w") as f:
            #for section_tuples in pre_processed_logs:
                #log_section = list(dict(section_tuples).values())
                #f.write("\t".join(log_section))
            try: 
                for log_tuple in pre_processed_logs:
                    f.write(log_tuple[1])

            except UnicodeEncodeError as e:
                print(e)
    else:
        pre_processed_logs = []
        
        # Load from file
        with open('mid_files/pre_processed_logs.txt') as f:
            for log_section in f.readlines():
                pre_processed_logs.append(log_section.split("\t"))

    #### Log lines' observation filtering
    obs_logs_procesing = True
    obs_logs = pre_processed_logs
    
    if obs_logs_procesing:
        obs_fetching = fetch_obs_file("obs_files", date)
        obs_list = open_obs_file("obs_files/{0}.csv".format(date))
        obs_logs = obs_filtering(logger, pre_processed_logs, obs_list)

        # Save observation logs to file
        with open('mid_files/observation_logs.txt', "w") as f:
            for section_tuples in obs_logs:
                log_section = list(dict(section_tuples).values())
                f.write("\t".join(log_section))
    else:
        obs_logs = []
        
        # Load from file
        with open('mid_files/observation_logs.txt') as f:
            for log_section in f.readlines():
                obs_logs.append(log_section.split("\t"))

    #### Log lines parsing
    
    log_parsing = True
    parsed_data = None

    if log_parsing:
        tplt_list = open_txt_file("{0}.txt".format(tplt_arch_name))
        parsed_data = log_parsing_regex(logger, obs_logs, tplt_list)

        # Save parsed data to file
        with open('mid_files/parsed_data.txt', "w") as f:
            for data_dict in parsed_data:
                parsed_values = json.dumps(data_dict)
                f.writelines(parsed_values+'\n')
    else:
        parsed_data = []
        
        # Load from file
        with open('mid_files/parsed_data.txt') as f:
            for data_dict in f.readlines():
                print(data_dict.split("\t"))
                parsed_data.append(data_dict.split("\t"))

    #### Parsed data saving
    #save_parsed_data(parsed_data, dest_arch_name) 

    #### Parsed data classifier
    df_f_dist, df_additional_data, df_corrections, df_images = generate_dataframes(logger, parsed_data)

    #### Linking df_images and fits files
    #df_images = link_images(logger, df_images, "img_files")

    #### Validate successful correction instances
    df_corrections = validate_corrections(logger, df_corrections, df_f_dist)

    #### Link forces distribution with correction instances
    df_corrections = link_dataframes(logger, df_corrections, df_f_dist, "f_dist", "timestamp")

    #### Link forces distribution with correction instances
    df_corrections = link_dataframes(logger, df_corrections, df_images, "img", "exposition_start")

    #### Save df in csv files
    df_f_dist.to_csv("dataframes/df_f_dist.csv", index=False)
    df_additional_data.to_csv("dataframes/df_additional_data.csv", index=False)
    df_corrections.to_csv("dataframes/df_corrections.csv", index=False)
    df_images.to_csv("dataframes/df_images.csv", index=False)

    print(df_f_dist)
    print(df_additional_data)
    print(df_corrections)
    print(df_images)
