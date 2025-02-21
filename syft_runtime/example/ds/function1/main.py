import os

data_path = os.getenv("DATA_PATH")
output_folder = os.getenv("OUTPUT_DIR")


def message_len():
    with open(data_path, "r") as f:
        message = f.read()

    # write result
    with open(os.path.join(output_folder, "message_len.txt"), "w") as f:
        f.write(str(len(message)))

    # call home
    # r = requests.get("https://example.com/")
    # print(r.text)
    return len(message)


print("Lenght of message: ", message_len())
