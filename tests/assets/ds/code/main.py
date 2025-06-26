import os

DATA_DIR = os.environ["DATA_DIR"]
OUTPUT_DIR = os.environ["OUTPUT_DIR"]


with open(os.path.join(DATA_DIR, "data.csv"), "r") as f:
    print(f.read())

with open(os.path.join(OUTPUT_DIR, "my_result.csv"), "w") as f:
    f.write("Hello, world!")


def main():
    print("Hello from test job!")


if __name__ == "__main__":
    main()
