import json

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