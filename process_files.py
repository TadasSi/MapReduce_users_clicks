import csv
import pandas as pd
import multiprocessing as mp
import numpy as np
from collections import defaultdict


def process_file_pd(file_name):
    """
    Read files into Pandas dataframe
    :param file_name: full path of file name
    :return: pandas dataframe
    """
    try:
        df = pd.read_csv(file_name)
        return df
    except OSError as e:
        print('Error' + str(e))
        raise


def parallelize_dataframe(df, func, num_partitions):
    """
    Split the dataframe into the set of partitions
    :param df: dataframe
    :param func: function being applied to the dataframe
    :param num_partitions: number of partitions to split dataframe
    :return: dataframe
    """

    df_split = np.array_split(df, num_partitions)
    pool = mp.Pool(num_partitions)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()

    return df


def exp_total_clicks(df_clicks, file_path):
    """
    Counts clicks and exports dataframe to csv file
    :param df_clicks: dataframe
    :param file_path: file path to export data
    :return: None
    """
    clicks_count = df_clicks.groupby(['date']).size().reset_index(name='count')

    try:
        clicks_count.to_csv(file_path, columns=["date", "count"], header=True, index=False, sep=",",
                            line_terminator="\n")
    except Exception as e:
        print('Error' + str(e))
        raise


def exp_filtered_clicks(df_clicks, df_users, country, file_path):
    """

    :param df_clicks: Dataframe for clicks data
    :param df_users: Dataframe for users data
    :param country: filter for country
    :param file_path: file path to export data
    :return: None
    """
    try:
        df_union = pd.merge(df_clicks, df_users, left_on="user_id", right_on="id", how="inner")
        df_filtered = df_union[df_union["country"].str.upper() == country.upper()].drop("id", 1)

        df_filtered.to_csv(file_path, header=True, index=False, sep=",", line_terminator="\n")
    except Exception as e:
        print('Error' + str(e))
        raise


def read_files_to_df(clicks_file_list, users_file_list, max_workers):
    """
    Read data files to dataframes in parallel
    :param clicks_file_list: files list for clicks data
    :param users_file_list: files list for users data
    :param max_workers: number of cores on your machine
    :return: clicks and users dataframes
    """

    with max_workers as pool:
        df_clicks = pool.map(process_file_pd, clicks_file_list)
        df_clicks = pd.concat(df_clicks, ignore_index=True, sort=False)
        df_users = pool.map(process_file_pd, users_file_list)
        df_users = pd.concat(df_users, ignore_index=True, sort=False)

    return df_clicks, df_users


def read_files(root, file_name):
    if file_name.endswith(".csv"):
        process_file(root + file_name)


def process_file(file_name):
    print('Processing file:' + file_name)
    with open(file_name, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                print([row])
                line_count += 1
            print(f'\tIn {row["date"]} user {row["user_id"]} clicks {row["click_target"]}.')
            line_count += 1
        print(f'Processed {line_count} lines.')


def write_file(directory, file_name, data):
    full_file_path = directory + file_name

    with open(full_file_path, mode='w', newline='') as csv_file:
        fieldnames = ['date', 'count']
        csv.unix_dialect()
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(data)


def map_reduce_light(my_input, mapper, reducer):
    map_results = map(mapper, my_input)
    shuffler = defaultdict(list)
    for key, value in map_results:
        shuffler[key].append(value)
    return map(reducer, shuffler.items())
