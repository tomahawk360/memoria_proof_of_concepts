from datetime import datetime
import json
import time
import logging
import pandas as pd
import csv
import argparse
import sys

from modules.open_txt_file import open_txt_file
from modules.log_formatting import log_formatting
from modules.fetch_obs_file import fetch_obs_file
from modules.open_obs_file import open_obs_file
from modules.obs_filtering import obs_filtering
from modules.log_parsing import log_parsing_regex
from modules.generate_dataframes import generate_dataframes
from modules.validate_images import validate_images
from modules.validate_forces import validate_forces
from modules.link_images import link_images
from modules.validate_corrections import validate_corrections
from modules.link_dataframes import link_dataframes
from modules.save_df_as_csv import save_df_as_csv
from modules.save_report import save_report
from modules.print_df_console import print_df_console
from modules.parse_args import parse_args


"""
Global variables:
    tracking_time: Initialization of time for checkpoint monitoring
"""
tracking_time = time.time()


def process_path(logger: logging.Logger, args: argparse.Namespace) -> None:
    """Order of execution of the algorithm major stages

    Args:
        logger: Current script logging object
        args: Namespace with the defined arguments and optional flags

    Returns:
        None
    """

    #### Original log lines
    log_lines = open_txt_file("../files/logs/wt{0}tcs.{1}.log".format(args.ut, args.date))

    #### Subpath that generates the dataframes
    df_list, mid_list = dataframe_generation_subpath(
        logger=logger,
        log_lines=log_lines,
        date=args.date,
        tplt_arch_flag=args.templatefile,
        pre_process_flag=args.preprocess,
        obs_process_flag=args.obsprocess,
    )

    #### Subpath that refines and stores the dataframes
    refined_df_list = dataframe_refining_subpath(
        logger=logger,
        df_list=df_list,
        img_linking=args.linkimages,
        df_linking=args.linkdataframes,
    )

    dataframe_showcasing_subpath(
        logger=logger,
        log_file=log_lines,
        mid_files_list=mid_list,
        df_list=refined_df_list,
        console_flag=args.console,
        save_flag=args.save,
        report_flag=args.report,
    )


def dataframe_generation_subpath(
    logger: logging.Logger,
    log_lines: list[str],
    date: str,
    tplt_arch_flag: str,
    pre_process_flag: bool,
    obs_process_flag: bool,
) -> list[list[any]]:
    """Invokes the algorithm methods to pre-process the log lines, filter them by valid observations, parsed them and generate the 
    respective dataframes

    Args:
        logger: Current script logging object
        log_lines: List of the original log lines
        date: Date, in string format, of the night of the log file
        tplt_arch_flag: Flag, and name if true, for the use of a customized template filename
        pre_process_flag: Flag for the use of a pre-processing stage
        obs_process_flag: Flag for the use of an observation filtering stage

    Returns:
        List of lists, the first of generated dataframes and the second of the intermediate algorithm stages
    """

    #### Log lines pre-processing
    pre_processed_logs = None

    if pre_process_flag:
        pre_processed_logs = log_formatting(logger, log_lines)

        # Save pre-processed logs to file
        with open("../files/mid_files/pre_processed_logs.txt", "w", encoding="utf-8") as f:
            for log_line in pre_processed_logs:
                f.write("\t" + log_line)

    else:
        pre_processed_logs = log_lines

        # Save pre-processed logs to file
        with open("../files/mid_files/pre_processed_logs.txt", "w", encoding="utf-8") as f:
            for log_line in pre_processed_logs:
                f.write("\t" + log_line)

    #### Log lines' observation filtering
    obs_logs = pre_processed_logs

    if obs_process_flag:
        fetch_obs_file("../files/obs_files", date)
        obs_list = open_obs_file("../files/obs_files/{0}.csv".format(date))
        obs_logs = obs_filtering(logger, pre_processed_logs, obs_list, date)

        # Save observation logs to file
        with open("../files/mid_files/observation_logs.txt", "w") as f:
            for obs_section_key in obs_logs.keys().__iter__():
                f.write("\t" + obs_section_key + "\t")
                for obs_line in obs_logs[obs_section_key]:
                    f.write("\t\t" + obs_line)

    #### Log lines parsing

    log_parsing = True
    parsed_data = None
    tplt_arch_name = "../files/templates/poc_templates"

    if tplt_arch_flag:
        tplt_arch_name = tplt_arch_flag

    if log_parsing:
        tplt_list = open_txt_file("{0}.txt".format(tplt_arch_name))
        parsed_data = log_parsing_regex(logger, obs_logs, tplt_list)

        # Save parsed data to file
        with open("../files/mid_files/parsed_data.txt", "w") as f:
            for data_dict in parsed_data:
                parsed_values = json.dumps(data_dict)
                f.writelines(parsed_values + "\n")
    else:
        parsed_data = []

        # Load from file
        with open("../files/mid_files/parsed_data.txt") as f:
            for data_dict in f.readlines():
                print(data_dict.split("\t"))
                parsed_data.append(data_dict.split("\t"))

    #### Parsed data classifier
    df_list = generate_dataframes(logger, parsed_data)

    #### Mid files list
    mid_files_list = [pre_processed_logs, obs_logs, parsed_data]

    return [df_list, mid_files_list]


def dataframe_refining_subpath(
    logger: logging.Logger,
    df_list: list[pd.DataFrame],
    img_linking: bool,
    df_linking: bool,
) -> list[pd.DataFrame]:
    """Invokes the algorithm methods to validate the format of the dataframes for images, force distributions and correction instances,
    linkes the image dataframe with the respective fits files, and links the correction dataframe with the respective force distribution
    and image for each row. 
    respective dataframes

    Args:
        logger: Current script logging object
        df_list: List of the previously generated dataframes
        img_linking: Flag for the linking of the image dataframe with the respective fits file for each row
        df_linking: Flag for the linking of the correction dataframe with the respective force distribution and image for each row

    Returns:
        List of the refined dataframes
    """

    #### Denormalizing dataframe list
    df_corrections, df_f_dist, df_images, df_additional_data = df_list

    #### Validate successful images instances
    df_images = validate_images(logger, df_images)

    #### Validate successful force distribution instances
    df_f_dist = validate_forces(logger, df_f_dist, df_images)

    #### Linking df_images and fits files
    if img_linking:
        df_images = link_images(logger, df_images, "img_files")

    #### Validate successful correction instances
    df_corrections = validate_corrections(logger, df_corrections, df_f_dist)

    #### Link forces distributions and images with correction instances
    if df_linking:
        df_corrections = link_dataframes(
            logger, df_corrections, df_f_dist, "f_dist", "timestamp"
        )
        df_corrections = link_dataframes(
            logger, df_corrections, df_images, "img", "exposition_start"
        )

    #### Refined dataframe list
    nu_df_list = [df_corrections, df_f_dist, df_images, df_additional_data]

    return nu_df_list


def dataframe_showcasing_subpath(
    logger: logging.Logger,
    log_file: list[str],
    mid_files_list: list[list[str]],
    df_list: list[pd.DataFrame],
    console_flag: bool,
    save_flag: str,
    report_flag: str,
) -> None:
    """Invokes the algorithm methods to save the generated dataframes as a csv file, print on console or generate a report file to showcase
    the algorithm performance

    Args:
        logger: Current script logging object
        log_file: List of the original log files
        mid_files_list: List of the intermediate algorithm stages results
        df_list: List of the previously generated dataframes
        console_flag: Flag for the use of the console stage
        save_flag: Flag for the use of the save as csv stage
        report_flag: Flag for the use of the report file stage

    Returns:
        None
    """

    #### Denormalizing dataframe list
    #### Save dataframes as csv
    if save_flag:
        save_df_as_csv(df_list, save_flag)

    #### Print dataframes in console
    if console_flag:
        print_df_console(df_list)

    #### Print dataframes in console
    if report_flag:
        save_report(
            log_file,
            mid_files_list,
            "Reporte {0}".format(datetime.now().strftime("%Y-%m-%d_%H-%M-%S")),
        )



def main(argv: list[str]):
    #### MVP Logger
    logging.basicConfig(filename="../files/poc.log", level=logging.INFO, filemode="w")
    logger = logging.getLogger("myLogger")

    #### Timer
    init_time = time.time()

    #### Arg parser
    args = parse_args(argv)

    #### Processing path
    process_path(logger, args)

    ### Final time
    print("Final time: ", str(time.time() - init_time))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
