import re
import json
import csv
import time
import xml
import logging
import tracemalloc
from ttp import ttp


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
        
        for line in reader:
            tpl_list.append((line[11], line[12].split("T")[1]))

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
Removes the header of each log line and groups them in blocks, according the timeframe between the VLT autoguider stopping an iteration
and the beggining of the following iteration, which corresponds to an observation period.

Args:
    logger: Current script logging object.
    log_lines: List of log lines, in string form.

Returns:
    A list of lists, with each containing a group of headerless log lines, fitted by observation period.
"""
def log_pre_processing(logger, log_lines):
    global tracking_time

    ### Checkpoint - Start log pre-processing
    logger.info('Start log pre-processing: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()
    
    headerless_lines = []
    extraction_flag = False

    for index, line in enumerate(log_lines):
        
        # Log line header removal
        header_re = re.compile(r'([a-zA-Z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}?)\swt1tcs\s(.+)(\[[0-9]+\]):\s')

        headerless_line = re.sub(header_re, '', line)

        if(headerless_line.find("AG.GUIDE") != -1):
            ### Checkpoint - AG.GUIDE line found
            logger.info('AG.GUIDE line found: {0}'.format(str(time.time() - tracking_time)))
            tracking_time = time.time()

            if(headerless_line.find("STOP") != -1):
                # Start log line extraction
                headerless_lines.append([])
                extraction_flag = True

            elif(headerless_line.find("START") != -1):
                # Finish log line extraction
                if(len(headerless_lines) != 0):
                    headerless_lines[-1].append((index, headerless_line))

                extraction_flag = False

        if(extraction_flag): 
            headerless_line = headerless_line.replace("  ", " ")
            headerless_lines[-1].append((index, headerless_line))

            ### Checkpoint - Line added to current list
            logger.info('Line added to current list: {0}'.format(str(time.time() - tracking_time)))
            tracking_time = time.time()

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
    A list of lists, with each containing a group of headerless log lines, fitted by observation period with successful observations.
"""
def obs_filtering(logger, log_lines, obs_list):
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
In Development

Args:

Returns:

"""
def template_generator(logger, templates_list):
    ### Checkpoint
    logger.info('Inicio de generador de plantillas: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    nu_template_list = []

    for template in templates_list:
        template_obj = {}

        if(template.find("{{date_body}}") != -1):
            template_obj['date_body'] = r"[0-2][0-9]{3}-[0-1][0-9]-[0-3][0-9]"
            template = str.replace(template, "{{date_body}}", r"[0-2][0-9]{3}-[0-1][0-9]-[0-3][0-9]")

        if(template.find("{{time_body}}") != -1):
            template_obj['time_body'] = r"[0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]+)*"
            template = str.replace(template, "{{date_body}}", r"[0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]+)*")
            
        template = re.sub(r"{{[0-9]+}}", r"[0-9.-]+", template, 1)


    return nu_template_list


"""
Parsess each log line and extracts their dynamic data, using the Text Template Parsing library (ttp).

Args:
    logger: Current script logging object.
    log_sections: List of lists of log lines, grouped by successful observation period, in string form.
    templates_list: List of log line parsing templates, in string form.

Returns:
    A list of dictionaries, with each containing the dynamic data extracted from a log line.
"""
def log_parsing_ttp(logger, log_sections, templates_list):
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
    log_sections: List of lists of log lines, grouped by successful observation period, in string form.
    templates_list: List of log line parsing templates, in string form.

Returns:
    A list of dictionaries, with each containing the dynamic data extracted from a log line.
"""
def log_parsing_regex(logger, log_sections, templates_list):
    global tracking_time

    ### Checkpoint - Start log line parsing
    logger.info('Start log line parsing: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    parsed_data = []

    for log_section in log_sections:
        for index, line in log_section:
            result = {}

            #Iterates through all templates
            for template in templates_list:

                template = template.replace("\n", "")

                #Parsing with regular expressions
                parser = re.search(template, line)

                # If match is true, the data extracted is saved and jumps into the next log line
                if(parser is not None):

                    parser_res = parser.group()

                    # Parses a log line's date
                    check_date = re.search(r"[0-2][0-9]{3}-[0-1][0-9]-[0-3][0-9]", parser_res)
                    if(check_date is not None):
                        result["date_body"] = check_date.group()
                        parser_res = re.sub(r"[0-2][0-9]{3}-[0-1][0-9]-[0-3][0-9]", '', parser_res)

                    # Parsees a log line's hour time
                    check_time = re.search(r"[0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]+)*", parser_res)
                    if(check_time is not None):
                        result["time_body"] = check_time.group()
                        parser_res = re.sub(r"[0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]+)*", '', parser_res)
                    
                    # Parsees a log line's numeric data
                    check_numeric = re.findall(r"\s([0-9.-]+)", parser_res)
                    if(check_numeric is not None):
                        result["numberic_values"] = check_numeric

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
        logger.info('End iteration on log line group: {0}'.format(str(time.time() - tracking_time)))
        tracking_time = time.time()

    ### Checkpoint - End log line parsing
    logger.info('End log line parsing: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    return parsed_data

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
    logging.basicConfig(filename='P005.log', level=logging.INFO)
    logger = logging.getLogger('myLogger')

    ### Log date
    file_name = '2024-09-20'

    ### Relative path of destination file for parsed data
    dest_arch_name = 'P005_data.txt'

    ### Observations csv file extraction
    obs_arch_name = "wdb_query_{0}.csv".format(file_name)
    obs_list = open_obs_file(obs_arch_name)

    ### Log lines extraction
    log_arch_name = 'logs/wt1tcs.{0}.log'.format(file_name)
    log_lines = open_txt_file(log_arch_name)

    ### Templates extraction
    tplt_arch_name = 'P005_templates.txt'
    tplt_list = open_txt_file(tplt_arch_name)

    #### Log lines pre-processing
    headerless_logs = log_pre_processing(logger, log_lines)

    #### Log lines' observation filtering
    obs_logs = obs_filtering(logger, headerless_logs, obs_list)

    #### Log lines parsing
    parsed_data = log_parsing_regex(logger, obs_logs, tplt_list)

    #### Parsed data saving
    save_parsed_data(parsed_data, dest_arch_name)

