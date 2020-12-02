from inout import IO
from transaction_manager import TransactionManager
import sys



if len(sys.argv) < 2:
    print("Usage: python3 dba.py <inputfile>")
    sys.exit()

filename = sys.argv[1]

io = IO(filename)
tm = TransactionManager()

op = io.get_op()

while tm.execute(op):
    op = io.get_op()
