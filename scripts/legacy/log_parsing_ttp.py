import time
import re
import logging
import ttp
import xml


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