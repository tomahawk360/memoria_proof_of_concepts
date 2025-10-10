import csv

def open_obs_file(arch_name) -> list[list[str]]:
    """Opens the VLT Observations CSV file and filters out the header and irrelevant fields

    Args:
        arch_name: CSV file relative path

    Returns:
        A list of lists, with each lists containing the observation instrument, it's respective timestamp and the exptime
    """

    tpl_list = []

    with open(arch_name, mode="r") as query:
        reader = csv.reader(query)

        next(reader)

        for line in reader:
            if line[11] != "":
                tpl_list.append([line[10].split("_")[0], line[11], line[12]])

    tpl_list.pop(0)

    return tpl_list