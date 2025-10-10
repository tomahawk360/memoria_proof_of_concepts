import pandas as pd

def save_df_as_csv(df_list: list[pd.DataFrame], folder: str) -> None:
    """Saves parsed data in an external CSV file

    Args:
        df_list: List of the final dataframes
        folder: Name or path of the folder to store the CSV files

    Returns:
        None
    """

    df_corrections, df_f_dist, df_images, df_additional_data = df_list

    #### Save df in csv files
    df_f_dist.to_csv("{0}/df_f_dist.csv".format(folder), index=False)
    df_additional_data.to_csv("{0}/df_additional_data.csv".format(folder), index=False)
    df_corrections.to_csv("{0}/df_corrections.csv".format(folder), index=False)
    df_images.to_csv("{0}/df_images.csv".format(folder), index=False)