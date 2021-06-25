# MapReduce_users_clicks
The goal of this project is to process data files in csv format, perform some calculations and export them back to files.

a)  Having in mind `data/clicks` dataset with "date" column, count how many clicks there were for each date and write 
the results to `data/total_clicks` dataset with "date" and "count" columns.

b)  here are two datasets:

- `data/users` dataset with columns "id" and "country"
- `data/clicks` dataset with columns "date", "user_id" and "click_target"

To produce a new dataset called `data/filtered_clicks` that includes only those clicks that belong to users from 
Lithuania (`country=LT`).

# How to prepare for running

1. Download main main.py, process_files.py and config.json files.
2. Set your own variables in config.json file (local directories, number of parallelism, country etc).
   If number of parallelism not be set, we'll take number of cores on your machine.
3. Run main.py script

### How to run

Use this python command to run script:

```
{path to python dir i.e C:\BI\python\env\Scripts\python.exe} {path to downloaded main.py script i.e C:/tmp/MapReduce/main.py}
```
