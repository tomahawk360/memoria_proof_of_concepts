import time
import logging
import pandas as pd
from datetime import datetime, timedelta

def link_dataframes(
    logger: logging.Logger,
    df_corrections: pd.DataFrame,
    df_attr: pd.DataFrame,
    name_attr: str,
    time_field: str,
) -> pd.DataFrame:
    """Builds the relationships between df_corrections and an specified dataframe of related attributes.

    Args:
        logger: Current script logging object.
        df_corrections: Dataframe of timestamps of corrections.
        df_attr: Dataframe of the specified attribute.
        name_attr: Name of the attribute to relate with the corrections.
        time_field: Name of the time column of the specified dataframe.

    Returns:
        df_corrections: Dataframe of corrections' timestamps and related ids
    """
    global tracking_time

    ### Checkpoint - Start link dataframes
    logger.info("Start link dataframes: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    halfday = datetime.strptime("12:00:00", "%H:%M:%S").time()

    num_corrections = len(df_corrections["id_corr"])

    # New temporary column for better conditioning
    buffer_c_day = datetime(1970, 1, 1)

    try:
        df_attr["unix"] = df_attr[time_field].apply(
            lambda x: (
                datetime.combine(buffer_c_day, x).timestamp()
                if x >= halfday
                else datetime.combine(buffer_c_day + timedelta(days=1), x).timestamp()
            )
        )

        # Set a maximum boundary from the max time in df_attr's unix column
        upper_boundary = df_attr["unix"].iloc[-1]

        for index in range(num_corrections):

            # Time of current correction
            curr_time = df_corrections.loc[index, "timestamp"]

            # Link with attribute
            max_f_time = datetime.strptime("00:00:00", "%H:%M:%S").time()

            # Series indexes
            old_id = "id_{0}_old".format(name_attr)
            new_id = "id_{0}_new".format(name_attr)
            attr_id = "id_{0}".format(name_attr)

            # Add unix column for better conditioning
            buffer_day = datetime(1970, 1, 1)

            if curr_time < halfday:
                buffer_day += timedelta(days=1)

            curr_unix = datetime.combine(buffer_day, curr_time).timestamp()

            for f_time in df_attr["unix"]:

                if curr_unix > f_time and upper_boundary > f_time:
                    max_f_time = f_time

                else:
                    if max_f_time == datetime.strptime("00:00:00", "%H:%M:%S").time():
                        after_index = df_attr["unix"].tolist().index(f_time)

                        df_corrections.loc[index, old_id] = -1
                        df_corrections.loc[index, new_id] = df_attr.loc[
                            after_index, attr_id
                        ]

                    else:
                        before_index = df_attr["unix"].tolist().index(max_f_time)
                        after_index = df_attr["unix"].tolist().index(f_time)

                        df_corrections.loc[index, old_id] = df_attr.loc[
                            before_index, attr_id
                        ]
                        df_corrections.loc[index, new_id] = df_attr.loc[
                            after_index, attr_id
                        ]

                    break

        # Remove temporary column
        df_attr.drop("unix", axis=1, inplace=True)

    except IndexError:
        print("Dataframe for {0} is empty. No linking done.".format(name_attr))

    ### Checkpoint - End link dataframes
    logger.info("End link dataframes: {0}".format(str(time.time() - tracking_time)))
    tracking_time = time.time()

    # return pd.DataFrame(dict_corrections)
    return df_corrections