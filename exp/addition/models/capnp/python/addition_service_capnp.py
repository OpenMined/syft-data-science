import capnp
from pathlib import Path

capnp_path = Path(__file__) / ".." /".."/ "addition.capnp"
capnp_path = str(capnp_path.resolve())

addition_capnp = capnp.load(capnp_path)

addition = addition_capnp.Addition.new_message(a=int(input("Enter A: ")),b=int(input("Enter B: ")))

result = addition.a + addition.b
print("Result: " , result)