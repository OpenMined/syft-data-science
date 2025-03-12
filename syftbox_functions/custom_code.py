def custom_code(input_path):
    with open(input_path, "r") as f:
        content = f.read()

    print(len(content.split(" ")))
    return len(content.split(" "))
