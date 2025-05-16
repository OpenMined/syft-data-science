import os

DATA_DIR = os.environ["DATA_DIR"]
OUTPUT_DIR = os.environ["OUTPUT_DIR"]

print("Hello, world!")

with open(os.path.join(OUTPUT_DIR, "output.txt"), "w") as f:
    f.write("ABC")
