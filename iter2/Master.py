#!/usr/local/bin/python3
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
import threading
from http.client import HTTPConnection
import concurrent.futures

import json
#remove:
import time

log = []
secondaries_dns = ['Secondary1', 'Secondary2']
secondary_port = 8081

class MasterConnectionHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        # self.log = log_ref
        super().__init__(*args, **kwargs)

    def do_GET(self):
        # input('wait')
        if self.path == '/list':
            self.send_response(200)
            self.end_headers()
            response = '\n'.join(log) + '\n'
            self.wfile.write(response.encode())
            # input('wait2')
            return
        else:
            self.send_response(404)
            self.end_headers()
            response = 'Not found\n'
            self.wfile.write(response.encode())
            return

    def do_POST(self):
        # print(self.headers)
        if self.path == '/append':
            content_len = int(self.headers.get('Content-Length'))
            request = json.loads(self.rfile.read(content_len))
            write_concern = int(request['w'])
            print(f'Got append request for "{request["msg"]}" with write concern {write_concern}')

            if write_concern >= 1 and write_concern <= 3:
                n_secondaries_to_request = write_concern - 1
                # got_acks = 0
                s1_done, s2_done = threading.Event(), threading.Event()
                s1_thread = threading.Thread(target=MasterConnectionHandler.replicate, args=(secondaries_dns[0], request["msg"], s1_done), daemon=True)
                s2_thread = threading.Thread(target=MasterConnectionHandler.replicate, args=(secondaries_dns[1], request["msg"], s2_done), daemon=True)
                s1_thread.start()
                s2_thread.start()
                while True:
                    if n_secondaries_to_request == 0:
                        print("Write concern is 1, not waiting for ACKs")
                        break
                    elif n_secondaries_to_request == 1 and (s1_done.is_set() or s2_done.is_set()):
                        print("Got 1/1 ACKs")
                        break
                    elif n_secondaries_to_request == 2 and s1_done.is_set() and s2_done.is_set():
                        print("Got 2/2 ACKs")
                        break

            elif write_concern > 3 or write_concern < 0:
                print('Unsupported write_concern value ', write_concern)
                self.send_response(400)
                self.end_headers()
                response = 'Bad request\n'
                self.wfile.write(response.encode())
                return

            # threading.Lock().acquire()
            global log
            log.append(request["msg"])
            # threading.Lock().release()

            self.send_response(200)
            self.end_headers()
            response = f'Appended "{request["msg"]}" to the log list\n'
            self.wfile.write(response.encode())
            print(response, end='')
            return
        else:
            self.send_response(404)
            self.end_headers()
            response = 'Not found\n'
            self.wfile.write(response.encode())
            return

    @staticmethod
    def replicate(secondary_dns, msg, resp_received_event):
        print(f'Sending replicate request for "{msg}" message to {secondary_dns}')
        c = HTTPConnection(secondary_dns, secondary_port) # blocking replication -> no timeout
        # c = HTTPConnection(secondary_dns, secondary_port, timeout=0.1)
        # handle error?
        # if request blocks ?
        # retries ? -> could do here by catching timeout exceptions
        c.request('POST', '/replicate', body=f'{{"msg": "{msg}"}}')
        status = c.getresponse().status # waits for timeout seconds
        print(f'Got {"ACK" if status == 200 else "NACK"} trying to replicate {msg} to {secondary_dns}')
        resp_received_event.set()

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""

class Master:
    def __init__(self, my_dns, port):
        self.my_dns = my_dns
        self.port = port
        
    def start(self):
        http_server = ThreadedHTTPServer((self.my_dns, self.port), MasterConnectionHandler)
        print('Starting server, use <Ctrl-C> to stop')
        http_server.serve_forever()

master = Master('Master', 8080)
master.start()