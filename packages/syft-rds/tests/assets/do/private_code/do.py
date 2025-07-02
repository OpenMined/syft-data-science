import os

INPUT_FILE = os.environ["INPUT_FILE"]  # user input file
OUTPUT_DIR = os.environ["OUTPUT_DIR"]  # job output directory
SECRET_KEY = os.environ["SECRET_KEY"]  # secret key for some api


def special_string_length(query: str):
    return len(query + SECRET_KEY)


with open(os.path.join(OUTPUT_DIR, "output.txt"), "w") as f:
    res = special_string_length("Hello, world!")
    f.write(f"Query result: {res}")
    print(f"Query result: {res}")
