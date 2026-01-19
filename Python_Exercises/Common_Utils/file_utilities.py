import os

def read_file(file_path, encoding_scheme="utf-8"):
    input_file = open(file_path, 'r', encoding=encoding_scheme)
    return input_file.read()

def write_file(file_path, contents, encoding_scheme="utf-8"):
    with open(file_path, 'w', encoding=encoding_scheme) as output_file:
        output_file.write(contents)

def make_directory_if_not_exists(file_path):
    if not os.path.exists(file_path):
        os.makedirs(file_path)