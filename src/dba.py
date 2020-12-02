from inout import IO
from transaction_manager import TransactionManager

print("Please enter the filename for input file (must be under the data folder).")

filename = "../data/" + input("Filename: ")


io = IO(filename)
tm = TransactionManager()

op = io.get_op()

while tm.execute(op):
    op = io.get_op()
