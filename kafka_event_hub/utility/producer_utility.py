import re
from datetime import datetime
from os import sep, listdir, system
from os.path import isfile, join, isdir



detailed_granularity_pattern = re.compile('Thh:mm:ssZ', re.UNICODE | re.DOTALL | re.IGNORECASE)


def current_utc_timestamp(granularity: str = None):
    if granularity is not None and detailed_granularity_pattern.search(granularity):
        current_time_utc = datetime.utcnow()
        return '{}T{:0>2}:{:0>2}:{:0>2}Z'.format(current_time_utc.date(), current_time_utc.hour, current_time_utc.minute, current_time_utc.second)
    else:
        # granularity without time => next from should be the current day (think about it again especially with UTC)
        return datetime.utcnow().strftime("%Y-%m-%d")


def current_timestamp():
    current_time = datetime.now()
    return '{}T{:0>2}:{:0>2}:{:0>2}Z'.format(current_time.date(), current_time.hour, current_time.minute, current_time.second)


def transform_from_until(value, granularity ='YYYY-MM-DDThh:mm:ssZ'):
    if detailed_granularity_pattern.search(granularity):
        if isinstance(value, str):
            return '{:%Y-%m-%dT%H:%M:%SZ}'.format(
                datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ'))
        else:
            return '{:%Y-%m-%dT%H:%M:%SZ}'.format(
                value, '%Y-%m-%dT%H:%M:%SZ')
    else:
        if isinstance(value, str):
            return '{:%Y-%m-%d}'.format(
                datetime.strptime(value, '%Y-%m-%d'))
        else:
            return '{:%Y-%m-%d}'.format(
                value, '%Y-%m-%d')

def add_end_dir_separator(directory) -> str:
    if directory[-1:] == sep:
        return directory
    else:
        return directory + sep


def remove_end_dir_separator(directory) -> str:
    if directory[-1:] == sep:
        return directory[:-1]
    else:
        return directory


def list_only_files(dir, glob="^.*$"):
    temp = add_end_dir_separator(dir)
    onlyfiles = [f for f in listdir(temp) if isfile(join(temp, f))]
    pattern = re.compile(glob,re.UNICODE | re.DOTALL | re.IGNORECASE)
    filtered_files = filter(lambda file: pattern.search(file), onlyfiles)
    return filtered_files


def list_only_files_absolute_path(dir, glob="^.*$"):
    temp = list_only_files(dir, glob)
    onlyfiles_abs = list(map(lambda filename: add_end_dir_separator(dir) + filename, temp))
    return onlyfiles_abs



def remove_files_from_dir(dir, glob="^.*$"):
    onlyfiles = list_only_files_absolute_path(dir,glob)
    for file in onlyfiles:
        system("rm " + file)


def space():
    return " "


def move_files(from_dir,to_dir,glob="^.*$"):
    onlyfiles_abs = list_only_files_absolute_path(from_dir,glob)
    for file in onlyfiles_abs:
        system("mv " + file + space() + to_dir)


def cp_file(file,to_dir):
    system("cp " + file + space() + to_dir)


def list_files_absolute_sorted(dir, glob="^.*$"):
    files = [f for f in list_only_files_absolute_path(remove_end_dir_separator(dir)) if re.match(glob, f)]
    return files

def mkdir_if_absent(directory):
    if not isdir(directory):
       system("mkdir -p " + directory)
