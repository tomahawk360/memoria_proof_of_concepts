import re

def open_txt_file(arch_name):
    with open(file=arch_name, mode='r', encoding='iso-8859-1') as logs:
        lines = logs.readlines()

    return lines

def log_pre_processing(log_lines):
    headerless_lines = []

    for index, line in enumerate(log_lines):
        # Log line header removal
        header_re = re.compile(r'([a-zA-Z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}?)\swt1tcs\s(.+)(\[[0-9]+\]):\s')
        headerless_line = re.sub(header_re, '', line)

        if(headerless_line.find("START DET CHIP READ") != -1 and headerless_line.find("[lt4iaa]") != -1):
            headerless_line = headerless_line.replace("  ", " ")
            headerless_lines.append(headerless_line)

    return headerless_lines

### Relative path of destination file for parsed data
dest_arch_name = 'img_data.txt'

log_lines = open_txt_file('logs/wt4tcs.2025-08-04.log')
pre_processed_logs = log_pre_processing(log_lines)

with open(dest_arch_name, "w") as data_file:
    data_file.writelines(pre_processed_logs)