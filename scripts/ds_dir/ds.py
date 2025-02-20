import os
from time import sleep


data_dir = os.environ["DATA_DIR"]
sleep(1)
with open(os.path.join(data_dir, "data.csv"), "r") as f:
    print(f.read())
