import pandas as pd 

def print_df_console(df_list: list[pd.DataFrame]) -> None:
    """Print the dataframes on console

    Args:
        df_list: List of the final dataframes
    Returns:
        None
    """

    df_corrections, df_f_dist, df_images, df_additional_data = df_list

    #### Save df in csv files
    print(df_f_dist)
    print(df_additional_data)
    print(df_corrections)
    print(df_images)