import time
import logging
import pandas as pd

def validate_corrections(
    logger: logging.Logger, df_corrections: pd.DataFrame, df_f_dist: pd.DataFrame
):
    """Validates the format of the corrections dataframe's rows, by sorting them by timestamp column and removing null ids.
    It also filters out the correction instances without inmediate following force re-distribution instances

    Args:
        logger: Current script logging object
        df_corrections: Dataframe of timestamps of corrections
        df_f_dist: Dataframe of force distributions

    Returns:
        Dataframe of valid corrections
    """

    global tracking_time

    ### Checkpoint - Start validate corrections
    logger.info(
        "Start validate corrections: {0}".format(str(time.time() - tracking_time))
    )
    tracking_time = time.time()

    f_dist_id_list = df_f_dist["id_f_dist"].tolist()

    nu_df_corrections = df_corrections[
        df_corrections["id_f_dist_new"].isin(f_dist_id_list)
    ]

    nu_df_corrections = nu_df_corrections.sort_values(by="timestamp")
    nu_df_corrections = nu_df_corrections.reset_index(drop=True)

    ### Checkpoint - End validate correections
    logger.info(
        "End validate corrections: {0}".format(str(time.time() - tracking_time))
    )
    tracking_time = time.time()

    # return pd.DataFrame(dict_corrections)
    return nu_df_corrections