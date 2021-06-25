import os
import time
import multiprocessing as mp
import json

from process_files import read_files_to_df, exp_total_clicks, exp_filtered_clicks


def read_config():
    """
    Read config values, input and export directories, parallelism params
    :return: config values
    """
    with open("config.json") as config_file:
        config_data = json.load(config_file)
    input_clicks = config_data["input"]["input_clicks"]
    input_users = config_data["input"]["input_users"]
    parallelism_number = config_data["input"]["parallelism_number"]
    country = config_data["input"]["country"]
    output_total_clicks = config_data["output"]["output_total_clicks"]
    output_filtered_clicks = config_data["output"]["output_filtered_clicks"]

    return input_clicks, input_users, parallelism_number, country, output_total_clicks, output_filtered_clicks


def main():
    start = time.time()
    input_clicks, input_users, parallelism_number, country, output_total_clicks, output_filtered_clicks = read_config()

    max_workers = mp.Pool(processes=parallelism_number) if len(parallelism_number) > 0 else mp.Pool(processes=mp.cpu_count())
    country = country if len(country) > 0 else "LT"

    clicks_file_list = [input_clicks + f for f in os.listdir(input_clicks) if (f.endswith('.csv'))]
    users_file_list = [input_users + f for f in os.listdir(input_users) if (f.endswith('.csv'))]

    df_clicks, df_users = read_files_to_df(clicks_file_list, users_file_list, max_workers)

    exp_total_clicks(df_clicks, file_path=output_total_clicks+"total_clicks.csv")
    exp_filtered_clicks(df_clicks, df_users, country=country, file_path=output_filtered_clicks + country+"_clicks.csv")

    end = time.time()
    print('Completed in: %s sec' % (end - start))
    # print("End at:", datetime.fromtimestamp(time.time()))


if __name__ == '__main__':
    main()
