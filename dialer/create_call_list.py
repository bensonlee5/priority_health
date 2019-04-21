from datetime import datetime
from datetime import date

import numpy as np
import pandas as pd
import argparse


def calc_recency(df_input):
    """

    :param df_input: Input dataframe containing patient ID, action date, and action
    :return: dataframe containing recency in days for each patient ID
    """

    df_recency = pd.DataFrame()

    for a in df_input['action'].unique():
        if len(df_recency) == 0:
            df_recency = df_input.groupby('patient_id', as_index=False)[a].max()
            df_recency.rename(columns={a: "{}_most_rec".format(a)}, inplace=True)
        else:
            df_recency["{}_most_rec".format(a)] = df_input.groupby('patient_id')[a].max().values

    return df_recency


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generates call list for nurses')
    parser.add_argument('--file_date', help='dialer data file date', action='store_const')

    current_date = '20190421'
    dt = datetime.strptime(current_date, '%Y%m%d')
    df_raw = pd.read_csv('../raw/dialer_data{}.csv'.format(datetime.strftime(dt, '%y%m%d')),
                         parse_dates=['action_date'])

