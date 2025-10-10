import json
import re

def open_txt_file(arch_name):
    with open(file=arch_name, mode='r', encoding='iso-8859-1') as logs:
        lines = logs.readlines()

    return lines

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


log_lines = open_txt_file('mid_files/pre_processed_logs.txt')

in_readout_loop = 0

with open('inttime_wt{0}-{1}.txt'.format(ut, date), "w") as f:

    for index, line in enumerate(log_lines):

        # Log line header removal
        #header_re = re.compile(r'([a-zA-Z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}?)\swt1tcs\s(.+)(\[[0-9]+\]):\s')

        #headerless_line = re.sub(header_re, '', line)

        headerless_line = line

        parser = re.search(r'(wt[1-4]tcs ([0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]+)*)> DET EXP NO = [0-9]+)', headerless_line)

        if(headerless_line.find("TEL ACTO INTTIME") != -1):
            #if(headerless_line.find("TEL ACTO INTTIME = 45.0000000") == -1): 
            #    f.write(headerless_line)
            f.write(headerless_line)

        elif(parser is not None):
            f.write(headerless_line)
        
