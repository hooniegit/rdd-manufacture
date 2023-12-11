import socket
from datetime import datetime
from time import time, sleep

host = "localhost"
port = 9999

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((host, port))

for cnt in range (1, 101):
    start_time = time()
    
    nowdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = f"Nowdate is - {nowdate}, count - {cnt}"
    s.sendall(data.encode())
    
    end_time = time()
    sleep(1 - (end_time - start_time))

s.close()