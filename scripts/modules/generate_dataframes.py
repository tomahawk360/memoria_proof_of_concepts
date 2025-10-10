import time
import re
import logging
import math
import pandas as pd
from datetime import datetime, timedelta

def generate_dataframes(
    logger: logging.Logger, parsed_data: list[dict[str, str]]
) -> list[pd.DataFrame]:
    """Generates dataframes for force distributions, correction instances, images and additional data, based on a list of previously 
    parsed data

    Args:    
        logger: Current script logging object.
        parsed_data: List of dictionaries, each containing the log line dynamic parsed data

    Returns:
        List of dataframes for correction instances, force distributions, images and additional data, in that order.
    """

    global tracking_time

    ### Checkpoint - Start dataframes generation
    logger.info(
        "Start dataframes generation: {0}".format(str(time.time() - tracking_time))
    )
    tracking_time = time.time()

    dict_images = {
        "id_img": [],
        "exposition_start": [],
        "integration_time": [],
        "readout_start": [],
        "readout_stop": [],
        "ccd": [],
        "img_path": [],
    }

    dict_f_dist = {"id_f_dist": [], "forces": [], "timestamp": []}

    dict_additional_data = {
        "timestamp": [],
        "group": [],
        "label": [],
        "type": [],
        "value_str": [],
        "value_float": [],
        "value_int": [],
    }

    dict_corrections = {
        "timestamp": [],
        "id_f_dist_old": [],
        "id_f_dist_new": [],
        "id_img_old": [],
        "id_img_new": [],
    }

    # Parses a log line's date
    for line in parsed_data:

        if line["group"] == "FORCES":

            if line["label"] == "f_id":
                dict_f_dist["id_f_dist"].append(int(line["data"]))
                dict_f_dist["forces"].append([])
                dict_f_dist["timestamp"].append(
                    datetime.strptime(line["time"].split(".")[0], "%H:%M:%S").time()
                )

            elif line["label"] == "f_dist":
                f_dist_index = len(dict_f_dist["id_f_dist"]) - 1
                f_dist_content = line["data"].split()

                if f_dist_index >= 0:
                    for force in f_dist_content:
                        dict_f_dist["forces"][f_dist_index].append(float(force))

        elif line["group"] == "INIT":
            dict_corrections["timestamp"].append(
                datetime.strptime(line["time"].split(".")[0], "%H:%M:%S").time()
            )
            dict_corrections["id_f_dist_old"].append(None)
            dict_corrections["id_f_dist_new"].append(int(line["data"]))
            dict_corrections["id_img_old"].append(None)
            dict_corrections["id_img_new"].append(None)

        elif line["group"] == "IMAGE":

            if line["label"] == "INTTIME":
                dict_images["id_img"].append(None)
                dict_images["exposition_start"].append(
                    datetime.strptime(line["time"], "%H:%M:%S").time()
                )
                dict_images["integration_time"].append(int(line["data"]))
                dict_images["readout_start"].append(
                    datetime.strptime(line["time"], "%H:%M:%S").time()
                )
                dict_images["readout_stop"].append(
                    (
                        datetime.strptime(line["time"], "%H:%M:%S")
                        + timedelta(seconds=float(line["data"]))
                    ).time()
                )
                dict_images["ccd"].append(None)
                dict_images["img_path"].append(None)

            else:
                image_index = len(dict_images["id_img"]) - 1
                nu_img_flag = False

                if image_index == -1:
                    nu_img_flag = True

                else:
                    img_date = dict_images["exposition_start"][image_index]
                    buffer_min_day = datetime(1970, 1, 1)
                    acceptance_threshold = 2

                    if (
                        dict_images["integration_time"][image_index] is not None
                        and img_date
                        <= datetime.strptime(line["time"], "%H:%M:%S").time()
                        <= (
                            datetime.combine(buffer_min_day, img_date)
                            + timedelta(seconds=acceptance_threshold)
                        ).time()
                    ):
                        dict_images["id_img"][image_index] = int(line["data"])
                        dict_images["ccd"][image_index] = line["label"]

                    elif dict_images["integration_time"][image_index] is None:
                        nu_img_flag = True

                if nu_img_flag:
                    dict_images["id_img"].append(int(line["data"]))
                    dict_images["exposition_start"].append(
                        datetime.strptime(line["time"], "%H:%M:%S").time()
                    )
                    dict_images["integration_time"].append(None)
                    dict_images["readout_start"].append(
                        datetime.strptime(line["time"], "%H:%M:%S").time()
                    )
                    dict_images["readout_stop"].append(
                        (
                            datetime.strptime(line["time"], "%H:%M:%S")
                            + timedelta(seconds=float(line["data"]))
                        ).time()
                    )
                    dict_images["ccd"].append(line["label"])
                    dict_images["img_path"].append(None)

        else:
            dict_additional_data["timestamp"].append(
                datetime.strptime(line["time"], "%H:%M:%S").time()
            )
            dict_additional_data["group"].append(line["group"])
            dict_additional_data["label"].append(line["label"])

            value = line["data"]

            check_value = re.search(r"^-*[0-9]+[.0-9]*$", value)
            if check_value is not None:
                if "." in value:
                    dict_additional_data["type"].append("float")
                else:
                    dict_additional_data["type"].append("int")
            else:
                dict_additional_data["type"].append("str")

            try:
                dict_additional_data["value_str"].append(str(value))
            except:
                dict_additional_data["value_str"].append(" ")

            try:
                dict_additional_data["value_float"].append(float(value))
            except:
                dict_additional_data["value_float"].append(-9999999.0)

            try:
                dict_additional_data["value_int"].append(int(math.floor(value)))
            except:
                dict_additional_data["value_int"].append(-9999999)

    # Assign id column to dataframes
    df_f_dist = pd.DataFrame(dict_f_dist)

    df_additional_data = pd.DataFrame(dict_additional_data)
    df_additional_data["id_addt_data"] = df_additional_data.index

    df_corrections = pd.DataFrame(dict_corrections)
    df_corrections["id_corr"] = df_corrections.index

    df_images = pd.DataFrame(dict_images)
    df_images["id_image"] = df_images.index

    ### Checkpoint - End dataframes generation
    logger.info(
        "End dataframes generation: {0}".format(str(time.time() - tracking_time))
    )
    tracking_time = time.time()

    return [df_corrections, df_f_dist, df_images, df_additional_data]

