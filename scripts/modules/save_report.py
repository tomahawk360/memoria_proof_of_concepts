def save_report(
    log_lines: list[str], mid_files_list: list[list[any]], file_name: str
) -> None:
    """Generates and stores report of the analyzed lines

    Args:
        log_lines: List of the original log lines
        mid_files_list: List of lists, each one with the results of intermediate algorithm steps
        file_name: Name of the report file

    Returns:
        None
    """

    # Denormalizing lists
    _, _, parsed_lines = mid_files_list

    num_log_lines = len(log_lines)

    num_parsed_lines = len(parsed_lines)

    num_corr_lines = 0
    num_img_lines = 0
    num_forces_lines = 0

    for line in parsed_lines:
        if line["group"] == "INIT":
            num_corr_lines += 1

        elif line["group"] == "FORCES":
            num_forces_lines += 1

        elif line["group"] == "IMAGE":
            num_img_lines += 1

    num_deleted_lines = num_log_lines - num_parsed_lines

    with open(file_name, "w") as reporte:
        reporte.write("Number of log lines read: " + str(num_log_lines) + "\n")
        reporte.write("Number of log lines removed: " + str(num_deleted_lines))
        reporte.write("Number of log lines parsed: " + str(num_parsed_lines) + "\n")
        reporte.write(
            "Number of AO correction instance log lines parsed: "
            + str(num_corr_lines)
            + "\n"
        )
        reporte.write(
            "Number of force re-distribution instance log lines parsed: "
            + str(num_forces_lines)
            + "\n"
        )
        reporte.write(
            "Number of image obtention instance log lines parsed: "
            + str(num_img_lines)
            + "\n"
        )