import time
import logging
import pandas as pd

def validate_forces(
    logger: logging.Logger, df_f_dist: pd.DataFrame, df_images: pd.DataFrame
) -> pd.DataFrame:
    """Validates the format of the force distribution dataframe's rows, by sorting them by timestamp column and removing null ids.
    It also filters force distribution instances by filtering out those without inmediate previous and after images

    Args:    
        logger: Current script logging object
        df_f_dist: Dataframe of force distributions
        df_images: Dataframe of images

    Returns:
        Dataframe of valid force distributions
    """

    global tracking_time

    ### Checkpoint - Start validate images
    logger.info("Start validate forces: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    nu_df_f_dist = df_f_dist.copy()
    img_times_buffer = df_images["exposition_start"].copy().tolist()
    f_dist_index = 0

    while f_dist_index < len(nu_df_f_dist["id_f_dist"]):
        remove_flag = True

        for img_times_index in range(len(img_times_buffer) - 1):
            curr_time = img_times_buffer[img_times_index]
            next_time = img_times_buffer[img_times_index + 1]

            if (
                curr_time < nu_df_f_dist["timestamp"][f_dist_index]
                and nu_df_f_dist["timestamp"][f_dist_index] < next_time
            ):
                remove_flag = False

                img_times_buffer.remove(curr_time)
                img_times_buffer.remove(next_time)

                break

        if remove_flag:
            f_dist_remove_index = nu_df_f_dist["id_f_dist"][f_dist_index]
            nu_df_f_dist = nu_df_f_dist[
                nu_df_f_dist["id_f_dist"] != f_dist_remove_index
            ]

        f_dist_index += 1

    nu_df_f_dist = nu_df_f_dist.sort_values(by="timestamp")
    nu_df_f_dist = nu_df_f_dist.reset_index(drop=True)

    ### Checkpoint - End validate images
    logger.info("End validate forces: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    # return pd.DataFrame(dict_corrections)
    return nu_df_f_dist