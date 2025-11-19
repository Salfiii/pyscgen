import os


def get_file_folder(file):
    return os.path.dirname(os.path.realpath(file))


def get_path_rel_to(file, rel_path):
    return os.path.join(get_file_folder(file), rel_path)


def get_data_path():
    return  os.path.join(get_file_folder(__file__), "./data/")

