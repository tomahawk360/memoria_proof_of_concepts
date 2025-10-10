import time
import re
import logging
from datetime import datetime, timedelta

def log_parsing_regex(
    logger: logging.Logger, log_sections: dict, templates_list: list[str]
) -> list[dict]:
    """Parses each log line and extracts their dynamic data, using regular expressions

    Args:
        logger: Current script logging object
        log_sections: List of log lines, grouped by successful observation period, in string form
        templates_list: List of log line parsing templates, in string form

    Returns:
        A list of dictionaries, with each containing the dynamic data extracted from a log line
    """

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
