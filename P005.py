import re
import json
import csv
import time
import xml
import logging
import tracemalloc
import pandas as pd
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
    A list of lists, with each containing a group of tuples, with the id of an observation first and 
    the pre-processed log line last, fitted by observation period.
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
    A list of lists, with each containing a group of headerless log lines, with the id of an observation first and 
    the pre-processed log line last, fitted by observation period with successful observations.
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
                    check_forces = re.search(r"m1as m1asSetGlbRel", parser.group())
                    if(check_forces is not None):

                        check_f_dist = re.search(r"Forces", parser.group())
                        if(check_f_dist is not None):
                            result["attribute"] = "forces"
                            result["label"] = "f_dist"
                            result["date"] = parser.group(2)
                            result["time"] = parser.group(3)
                            result["data"] = parser.group(6)

                        else:
                            result["attribute"] = "forces"
                            result["label"] = "f_id"
                            result["date"] = parser.group(2)
                            result["time"] = parser.group(3)
                            result["data"] = parser.group(5)

                    else:
                        result["time"] = parser.group(2)
                        result["attribute"] = parser.group(4)
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
        logger.info('End iteration on log line group: {0}'.format(str(time.time() - tracking_time)))
        tracking_time = time.time()

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
def classify_parsed_lines(logger, parsed_data):
    
    df_f_dist = {
        "ID_f_dist": [],
        "force_1": [], "force_2": [], "force_3": [], "force_4": [], "force_5": [], "force_6": [], "force_7": [], "force_8": [], "force_9": [], "force_10": [], "force_11": [], "force_12": [], "force_13": [], "force_14": [], "force_15": [], "force_16": [], "force_17": [], "force_18": [], "force_19": [], "force_20": [], "force_21": [], "force_22": [], "force_23": [], "force_24": [], "force_25": [],
        "force_26": [], "force_27": [], "force_28": [], "force_29": [], "force_30": [], "force_31": [], "force_32": [], "force_33": [], "force_34": [], "force_35": [], "force_36": [], "force_37": [], "force_38": [], "force_39": [], "force_40": [], "force_41": [], "force_42": [], "force_43": [], "force_44": [], "force_45": [], "force_46": [], "force_47": [], "force_48": [], "force_49": [], "force_50": [],
        "force_51": [], "force_52": [], "force_53": [], "force_54": [], "force_55": [], "force_56": [], "force_57": [], "force_58": [], "force_59": [], "force_60": [], "force_61": [], "force_62": [], "force_63": [], "force_64": [], "force_65": [], "force_66": [], "force_67": [], "force_68": [], "force_69": [], "force_70": [], "force_71": [], "force_72": [], "force_73": [], "force_74": [], "force_75": [],
        "force_76": [], "force_77": [], "force_78": [], "force_79": [], "force_80": [], "force_81": [], "force_82": [], "force_83": [], "force_84": [], "force_85": [], "force_86": [], "force_87": [], "force_88": [], "force_89": [], "force_90": [], "force_91": [], "force_92": [], "force_93": [], "force_94": [], "force_95": [], "force_96": [], "force_97": [], "force_98": [], "force_99": [], "force_100": [],
        "force_101": [], "force_102": [], "force_103": [], "force_104": [], "force_105": [], "force_106": [], "force_107": [], "force_108": [], "force_109": [], "force_110": [], "force_111": [], "force_112": [], "force_113": [], "force_114": [], "force_115": [], "force_116": [], "force_117": [], "force_118": [], "force_119": [], "force_120": [], "force_121": [], "force_122": [], "force_123": [], "force_124": [], "force_125": [],
        "force_126": [], "force_127": [], "force_128": [], "force_129": [], "force_130": [], "force_131": [], "force_132": [], "force_133": [], "force_134": [], "force_135": [], "force_136": [], "force_137": [], "force_138": [], "force_139": [], "force_140": [], "force_141": [], "force_142": [], "force_143": [], "force_144": [], "force_145": [], "force_146": [], "force_147": [], "force_148": [], "force_149": [], "force_150": []
    }

    df_additional_data = {
        "attribute": [],
        "label": [],
        "data": []
    }

    k = 0
    
    # Parses a log line's date
    for line in parsed_data:

        if(line["attribute"] == "forces"):

            if(line["label"] == "f_id"):
                df_f_dist["ID_f_dist"].append(line["data"])

            elif(line["label"] == "f_dist"):
                
                threshold_1 = len(df_f_dist["ID_f_dist"]) > len(df_f_dist["force_1"])
                threshold_2 = len(df_f_dist["ID_f_dist"]) > len(df_f_dist["force_26"])
                threshold_3 = len(df_f_dist["ID_f_dist"]) > len(df_f_dist["force_51"])
                threshold_4 = len(df_f_dist["ID_f_dist"]) > len(df_f_dist["force_76"])
                threshold_5 = len(df_f_dist["ID_f_dist"]) > len(df_f_dist["force_101"])
                threshold_6 = len(df_f_dist["ID_f_dist"]) > len(df_f_dist["force_126"])

                if threshold_1: k = 0
                elif threshold_2: k = 25
                elif threshold_3: k = 50
                elif threshold_4: k = 75
                elif threshold_5: k = 100
                elif threshold_6: k = 125

                for i in range(25):
                    f_dist_content = line["data"].split()
                    df_f_dist["force_{0}".format(i+k+1)].append(f_dist_content[i])


        else:
            df_additional_data["attribute"] = line["attribute"]
            df_additional_data["label"] = line["label"]
            df_additional_data["data"] = line["data"]

    return df_f_dist, df_additional_data

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
    date = '2024-09-20'
    ### Relative path of destination file for parsed data
    dest_arch_name = 'P005_data.txt'

    #### Log lines pre-processing  
    pre_processed_logs_procesing = True
    pre_processed_logs = None

    if pre_processed_logs_procesing:
        log_lines = open_txt_file('logs/wt1tcs.{0}.log'.format(date))
        pre_processed_logs = log_pre_processing(logger, log_lines)

        # Save pre-processed logs to file
        with open('mid_files/pre_processed_logs.txt', "w") as f:
            for section_tuples in pre_processed_logs:
                log_section = list(dict(section_tuples).values())
                f.write("\t".join(log_section))
    else:
        pre_processed_logs = []
        
        # Load from file
        with open('mid_files/pre_processed_logs.txt') as f:
            for log_section in f.readlines():
                pre_processed_logs.append(log_section.split("\t"))

    #### Log lines' observation filtering
    obs_logs_procesing = True
    obs_logs = None

    if obs_logs_procesing:
        obs_list = open_obs_file("wdb_query_{0}.csv".format(date))
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
    ### Templates extraction
    tplt_list = open_txt_file('P005_templates.txt')
    parsed_data = log_parsing_regex(logger, obs_logs, tplt_list)

    #### Parsed data saving
    save_parsed_data(parsed_data, dest_arch_name)

