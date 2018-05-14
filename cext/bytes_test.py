#!/usr/bin/env python
import thread
from sys import byteorder as sys_byteorder
gr = thread.get_request

thread.init()

#b = (1).to_bytes(2, byteorder=sys_byteorder)
b = b'\x00\x01\x00\x00\x05\x1f\xa2\x00\x00\x00\x00'
i = 1234
thread.put_request(b, i)
