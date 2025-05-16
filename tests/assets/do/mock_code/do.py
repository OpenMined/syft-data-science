import os

INPUT_FILE = os.environ["INPUT_FILE"]
OUTPUT_DIR = os.environ["OUTPUT_DIR"]
SECRET_KEY = "MOCK_KEY"


def special_string_length(query: str):
    return len(query + SECRET_KEY)


with open(os.path.join(OUTPUT_DIR, "output.txt"), "w") as f:
    res = special_string_length("Hello, world!")
    f.write(f"Query result: {res}")
    print(f"Query result: {res}")
