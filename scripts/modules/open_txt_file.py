def open_txt_file(arch_name) -> list[str]:
    """Opens a text file and deposits it's information into a list

    Args:
        arch_name: Text file relative path

    Returns:
        A list with all the lines as strings
    """

    with open(file=arch_name, mode="r", encoding="iso-8859-1") as logs:
        lines = logs.readlines()

    return lines