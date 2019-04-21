from datetime import datetime
from datetime import timedelta

import pandas as pd
import argparse


def calc_recency(df_input):
    """
    Calculates # of days since patient was last touched by specific action type
    If the patient has never been touched, 99999 is populated in recency field

    :param df_input: Input dataframe containing patient ID, action date, and action
    :return: dataframe containing recency in days for each patient ID
    """

    df_recency = pd.DataFrame()

    for a in action_list:
        to_merge = df_input[df_input['action'] == a].groupby('patient_id', as_index=False)['action_date'].max()
        to_merge.rename(columns={"action_date": "{}_most_rec".format(a)}, inplace=True)

        if len(df_recency) == 0:
            df_recency = to_merge
        else:
            df_recency = df_recency.merge(to_merge, on='patient_id', how='outer')

    df_recency['cur_date'] = pd.to_datetime(dt_cur)
    for a in action_list:
        df_recency['{}_days_since'.format(a)] = (df_recency['cur_date'] - df_recency['{}_most_rec'.format(a)]).dt.days
        df_recency['{}_days_since'.format(a)].fillna(9999, inplace=True)

    return df_recency


def calc_num_event(df_input, event_type, recency):
    """
    Calculates the number of events occurring within specific timeframe

    :param df_input: input dataframe containing patient ID, event type, recency in days
    :param event_type: type of event (call, visit, urgent visit, admission)
    :param recency: recency in days
    :return: dataframe containing number of events by patient ID for specified event type that occurred
             within <recency> number of days
    """

    cutoff_date = dt_cur - timedelta(days=recency)
    df_event_out = None

    try:
        df_event_out = df_input.loc[(df_input['action'] == event_type) &
                                    (df_input['action_date'] >= cutoff_date)].groupby('patient_id',
                                                                                      as_index=False)['action'].count()
    except KeyError as e:
        print("Could not find action {}".format(event_type))
        exit(2)

    return df_event_out.rename(columns={'action': '{0}_within_{1}_days'.format(event_type, recency)})


def merge_num_event(df_base, df_raw_input, event_type, recency):
    """

    :param df_base: base dataframe to merge event counts into
    :param df_raw_input: raw dataframe to compute event counts on
    :param event_type:
    :param recency:
    :return: base dataframe with event counts merged in
    """

    df_event = calc_num_event(df_raw_input, event_type, recency)
    if len(df_event) == 0:
        df_base['{0}_within_{1}_days'.format(event_type, recency)] = 0
    else:
        df_base = df_base.merge(df_event, on='patient_id', how='left')
        df_base['{0}_within_{1}_days'.format(event_type, recency)].fillna(0, inplace=True)

    return df_base


def create_dialer_order():
    return NotImplementedError


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generates call list for nurses')
    parser.add_argument('-file_date', help='dialer data file date (YYYYMMDD format)', action='store')
    args = parser.parse_args()
    # current_date = '20190421'

    if args.file_date:
        try:
            dt_cur = datetime.strptime(args.file_date, '%Y%m%d')
        except TypeError as e:
            print("Input dialer file date must be in YYYYMMDD format")
            exit(2)
    else:
        dt_cur = datetime.today().date()

    try:
        df_raw = pd.read_csv('../raw/dialer_data{}.csv'.format(datetime.strftime(dt_cur, '%y%m%d')),
                             parse_dates=['action_date'])
    except FileNotFoundError as e:
        print('dialer_data{}.csv file not found in raw directory.'.format(datetime.strftime(dt_cur, '%y%m%d')))
        exit(2)

    action_list = list(df_raw['action'].unique())
    df_out = calc_recency(df_raw)
    for a in action_list:
        df_out = merge_num_event(df_out, df_raw, a, 10)
        df_out = merge_num_event(df_out, df_raw, a, 30)
        df_out = merge_num_event(df_out, df_raw, a, 365)

    print(df_out.T)
