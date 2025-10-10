import argparse

def parse_args(args_list: list[str]) -> argparse.Namespace:
    """Parses arguments and optional flags for console execution

    Args:
        args_list: System console arguments

    Returns:
        Namespace with the defined arguments and optional flags
    """

    parser = argparse.ArgumentParser()

    parser.add_argument("date", type=str, help="Date to analyzed")

    parser.add_argument(
        "ut", type=str, help="Number of the UT telescope to be analyzed"
    )

    parser.add_argument(
        "-t",
        "--templatefile",
        type=str,
        help="Path to the text file where the templates are stored (if no name is written, ./poc_templates.txt will be set by default)",
    )

    parser.add_argument(
        "-p",
        "--preprocess",
        action="store_false",
        help="Skips logs pre-processing stage: removal of log headers and grouping of log lines in sections, based on when AG GUIDE has stopped activity",
    )

    parser.add_argument(
        "-o",
        "--obsprocess",
        action="store_false",
        help="Skips observation files processing stage: filtering of log lines based on time intervals where a valid observation has been performed",
    )

    parser.add_argument(
        "-i",
        "--linkimages",
        action="store_false",
        help="Skips linking of image log data with fits files: write of the fits files' path respective to each image row in the images' dataframe",
    )

    parser.add_argument(
        "-l",
        "--linkdataframes",
        action="store_false",
        help="Skips linking of correction dataframe with related dataframes: write of the ids of the force distribution and image respective to each row in the correction dataframe",
    )

    parser.add_argument(
        "-c",
        "--console",
        action="store_true",
        help="Prints resulting dataframes on console",
    )

    parser.add_argument(
        "-s", "--save", type=str, help="Stores resulting dataframes in csv files"
    )

    parser.add_argument(
        "-r",
        "--report",
        action="store_true",
        help="Prints a report detailing the processing of log lines",
    )

    args = parser.parse_args(args_list)

    return args