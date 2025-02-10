from addition_pb2 import Addition



addition_data = Addition(a=int(input("Enter A: ")),b=int(input("Enter B: ")))

result = addition_data.a + addition_data.b
print("Result:" , result)