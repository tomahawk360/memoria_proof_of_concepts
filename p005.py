from astropy.io import fits
import re
import math
import json
import csv
import time
import xml
import logging
import tracemalloc
import pandas as pd
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
        header_re = re.compile(r'([a-zA-Z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}?)\swt[1-4]tcs\s(.+)(\[[0-9]+\]):\s')

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
    
    ### Second iteration: Recovers lines with AG.GUIDE boundaries timestamps that appear  outside the AG.GUIDE blocks
    recovering_lines = []
    prev_time = ""

    for index, line in enumerate(log_lines):

        # Log line header removal
        header_re = re.compile(r'([a-zA-Z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}?)\swt[1-4]tcs\s(.+)(\[[0-9]+\]):\s')

        headerless_line + re.sub(header_re, '', line)

        for log_section in headerless_lines:
            stop_time log_section[0][1]
            start_line = log_section[-1][1]

            time_re = re.compile(r'[0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]+)*')

            stop_time = re.search(time_re,stop_line).group()
            start_time = re.search(time_re,start_line).group()

            curr_time = re.search(time_re, line).group()

            if(stop_time == curr_time or start_time == curr_time):
                print(curr_time)

                headerless_line = headerless_line.replace("  ", " ")

                if(curr_time == prev_time):
                    recovering_lines[-1].append(index, headerless_line))

                elif(len(recovering_lines) == 0 or curr_time != prev_time):
                    recovering_lines.append([(index, headerless_line)])
                    prev_time = curr_time


    print(recovering_lines)

    ### WIP: Add recovering_lines into the final headerless_lines
    final_lines = []

    for reco_line in recovering_lines:
        if(reco_line[0]):
            buffer_list.append(reco_line[1])

        else:
            start_or_stop_flag = reco_line[0]
            final_lines.append(buffer_list.reverse() + headerless_lines[])



    ### Checkpoint - End log pre-processing
    logger.info('End log pre-processing: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    return final_lines


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

                    #check_line_f_dist = re.search(r"SetGlbAbs", parser.group())
                    #if(check_line_f_dist is not None):
                    #    print(parser.group())

                    # Parses a log line's date
                    check_forces = re.search(r"SetGlbAbs", parser.group())
                    check_onecal = re.search(r"ONECAL", parser.group())
                    check_inttime = re.search(r"INTTIME", parser.group())
                    check_exp_no= re.search(r"EXP NO", parser.group())
                    

                    if(check_forces is not None):

                        check_f_dist = re.search(r"Forces", parser.group())
                        
                        if(check_f_dist is not None):
                            result["group"] = "forces"
                            result["label"] = "f_dist"
                            result["date"] = parser.group(2)
                            result["time"] = parser.group(3)
                            result["data"] = parser.group(6)

                        else:
                            result["group"] = "forces"
                            result["label"] = "f_id"
                            result["date"] = parser.group(2)
                            result["time"] = parser.group(3)
                            result["data"] = parser.group(5)

                    elif(check_onecal is not None):
                        result["time"] = parser.group(2)
                        result["group"] = parser.group(4)

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
def generate_dataframes(logger, parsed_data):
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
        "forces": []
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

    dict_p_available = {
        "pistons": []
    }

    dict_corrections = {
        "timestamp": [],
        "id_p_av" : [],
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

            elif(line["label"] == "f_dist"):
                f_dist_index = len(dict_f_dist["id_f_dist"]) - 1
                f_dist_content = line["data"].split()

                for force in f_dist_content:
                    dict_f_dist["forces"][f_dist_index].append(float(force))

        elif(line["group"] == "ONECAL"):
            dict_corrections["timestamp"].append(line["time"])
            dict_corrections["id_p_av"].append(None)
            dict_corrections["id_f_dist_old"].append(None)
            dict_corrections["id_f_dist_new"].append(None)
            dict_corrections["id_img_old"].append(None)
            dict_corrections["id_img_new"].append(None)

        elif(line["group"] == "IMAGE"):

            if(line["label"] == "INTTIME"):
                dict_images["id_img"].append(None)
                dict_images["exposition_start"].append(line["time"])
                dict_images["integration_time"].append(line["data"])
                dict_images["readout_start"].append(None)
                dict_images["readout_stop"].append(None)
                dict_images["ccd"].append(None)
                dict_images["img_path"].append(None)

            else:
                image_index = len(dict_images["id_img"]) - 1

                if(image_index != -1):
                    if (dict_images["exposition_start"][image_index] == line["time"]):
                        dict_images["id_img"][image_index] = line["data"]
                        dict_images["ccd"][image_index] = line["label"]

        else:
            dict_additional_data["timestamp"].append(line["time"])
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
def link_images(logger, ut, date, df_images):
    global tracking_time
    
    ### Checkpoint - Start link images
    logger.info('Start link images: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    length = len(df_images["id_img"]) 

    for i in range(length-1):
        print(df_images.loc[i])

        if(df_images.loc[i, "readout_start"] is not None):
            file_path = "img_files/UT{0}_N{1}-ONEIA-{2}T{3}.fits".format(ut, df_images.loc[i, "ccd"][-1].upper(), date, df_images.loc[i, "exposition_start"].replace(":", "_"))

            with fits.open(file_path) as hdul:
                # Print information about the FITS file structure
                hdul.info()

                # Access the primary HDU
                primary_hdu = hdul[0]

                # Get the header information
                header_info = primary_hdu.header
                fits_exp_start = header_info['DATE-OBS']
                fits_integration_time = header_info['EXPTIME']

        
    if(fits_exp_start == df_images.loc[i, "exposition_start"]):
        df_images.loc[i, "exposition_start"] = fits_exp_start

    if(fits_integration_time == df_images.loc[i, "integration_time"]):
        df_images.loc[i, "integration_time"] = fits_integration_time

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
def link_dataframes(logger, df_corrections, df_f_dist, df_images):
    global tracking_time
    
    ### Checkpoint - Start link dataframes
    logger.info('Start link dataframes: {0}'.format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    f_dist_id_series = df_f_dist['id_f_dist']
    images_id_series = df_images['id_image']

    length = len(df_corrections["timestamp"])

    #First correction
    df_corrections.loc[0, "id_f_dist_old"] = -1
    df_corrections.loc[0, "id_f_dist_new"] = f_dist_id_series[0]
    df_corrections.loc[0, "id_img_old"] = -1
    df_corrections.loc[0, "id_img_new"] = images_id_series[0]

    for i in range(1, length-1):
        df_corrections.loc[i, "id_f_dist_old"] = f_dist_id_series[i-1]
        df_corrections.loc[i, "id_f_dist_new"] = f_dist_id_series[i]
        df_corrections.loc[i, "id_img_old"] = images_id_series[i-1]
        df_corrections.loc[i, "id_img_new"] = images_id_series[i]

    #Last correction
    #df_corrections.loc[length - 1, "id_f_dist_old"] = f_dist_id_series[length-2]
    #df_corrections.loc[length - 1, "id_f_dist_new"] = -1
    #df_corrections.loc[length - 1, "id_img_old"] = images_id_series[length-2]
    #df_corrections.loc[length - 1, "id_img_new"] = -1

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
    logging.basicConfig(filename='P005.log', level=logging.INFO)
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
    ### Relative path of observation csvs
    obs_csv_name = params["observation_filename"]

    #### Log lines pre-processing  
    pre_processed_logs_procesing = True
    pre_processed_logs = None

    if pre_processed_logs_procesing:
        log_lines = open_txt_file('logs/wt{0}tcs.{1}.log'.format(ut, date))
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
    obs_logs = pre_processed_logs

    #if obs_logs_procesing:
        #obs_list = open_obs_file("obs_files/{0}.csv".format(obs_csv_name))
        #obs_logs = obs_filtering(logger, pre_processed_logs, obs_list)

        # Save observation logs to file
        #with open('mid_files/observation_logs.txt', "w") as f:
            #for section_tuples in obs_logs:
                #log_section = list(dict(section_tuples).values())
                #f.write("\t".join(log_section))
    #else:
        #obs_logs = []
        
        # Load from file
        #with open('mid_files/observation_logs.txt') as f:
            #for log_section in f.readlines():
                #obs_logs.append(log_section.split("\t"))

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
        with open('mid_files/observation_logs.txt') as f:
            for data_dict in f.readlines():
                parsed_data.append(data_dict.split("\t"))

    #### Parsed data saving
    #save_parsed_data(parsed_data, dest_arch_name) 

    #### Parsed data classifier
    df_f_dist, df_additional_data, df_corrections, df_images = generate_dataframes(logger, parsed_data)

    ### Linking df_images and fits files
    #df_images = link_images(logger, ut, date, df_images)

    #### Parsed data relater
    df_corrections = link_dataframes(logger, df_corrections, df_f_dist, df_images)

    print(df_f_dist)
    print(df_additional_data)
    print(df_corrections)
    print(df_images)
