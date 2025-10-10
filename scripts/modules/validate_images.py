import time
import logging
import pandas as pd

def validate_images(logger: logging.Logger, df_images: pd.DataFrame) -> pd.DataFrame:
    """Validates the format of the image dataframe's rows, by sorting them by timestamp column and removing null ids

    Args:
        logger: Current script logging object
        df_images: Dataframe of images

    Returns:
        Dataframe of valid images
    """    

    global tracking_time

    ### Checkpoint - Start validate images
    logger.info("Start validate images: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    nu_df_images = df_images.dropna(subset="id_img")

    nu_df_images = nu_df_images.sort_values(by="exposition_start")
    nu_df_images = nu_df_images.reset_index(drop=True)

    ### Checkpoint - End validate images
    logger.info("End validate images: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    # return pd.DataFrame(dict_corrections)
    return nu_df_images
