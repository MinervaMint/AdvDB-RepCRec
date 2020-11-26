from inout import IO
from transaction_manager import TransactionManager
import sys


# filename = "src/test.txt"
filename = "test.txt"

io = IO(filename)
tm = TransactionManager()

op = io.get_op()
# print(op)

while tm.execute(op):
    op = io.get_op()
    # print(op)
