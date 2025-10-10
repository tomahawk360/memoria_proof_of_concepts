import time
import logging
import os
import pandas as pd
from astropy.io import fits

def link_images(
    logger: logging.Logger, df_images: pd.DataFrame, img_folder: str
) -> pd.DataFrame:
    """Builds the relationships between df_images and the images fits archives

    Args:    
        logger: Current script logging object.
        df_images: Dataframe of images.

    Returns:
        nu_df_images: Dataframe of images with corrected information
    """

    global tracking_time

    ### Checkpoint - Start link images
    logger.info("Start link images: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    for img_name in os.listdir(img_folder):
        with fits.open("{0}/{1}".format(img_folder, img_name)) as hdul:
            # Print information about the FITS file structure
            # hdul.info()

            # Access the primary HDU
            primary_hdu = hdul[0]

            # Get the header information
            header_info = primary_hdu.header

            # img_date = header_info['DATE-OBS']
            inttime = header_info["EXPTIME"]
            exp_no = header_info["HIERARCH ESO DET EXP NO"]

            try:
                index_img_list = df_images["id_img"].tolist().index(int(exp_no))

            except ValueError:
                # print('No match: {0}'.format(img_name))
                continue

            else:
                if df_images["integration_time"][index_img_list] == int(inttime):
                    df_images.loc[index_img_list, "img_path"] = img_name

                else:
                    print("Different inttime: {0}".format(img_name))

    ### Checkpoint - End link images
    logger.info("End link images: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    return df_images