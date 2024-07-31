import os

def read_text_files_in_directory(directory_path):
    files_content = {}

    for filename in os.listdir(directory_path):
        if filename.endswith(".txt"):
            file_path = os.path.join(directory_path, filename)
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
                files_content[filename.rstrip('.txt')] = content

    return files_content

files_content = read_text_files_in_directory('SETTINGS')
