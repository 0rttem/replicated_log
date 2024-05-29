#!/usr/local/bin/python3
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
import threading
from http.client import HTTPConnection

import json
from sortedcontainers import SortedDict
#remove:
import time

# for total ordering
log = SortedDict()

transaction_id = -1
secondaries_dns = ['Secondary1', 'Secondary2']
secondary_port = 8081

def retry(func, *args):
    try:
        func(*args)
    except (TimeoutError, ConnectionRefusedError) as e:
        print(f'Will retry {func.__name__} with args={args} in 2s')
        time.sleep(2)
        retry(func, *args)

class MasterConnectionHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/list':
            self.send_response(200)
            self.end_headers()
            response = '\n'.join(map(lambda x: str(x), log.items())) + '\n'
            self.wfile.write(response.encode())
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

            global transaction_id
            # need to increment before sending replication requests to secondaries
            transaction_id += 1 # comment this to test deduplication on Secondaries
            # need local cuz global can be incremented by another client request while we are waiting for responses from secondaries
            local_transaction_id = transaction_id

            if write_concern >= 1 and write_concern <= 3:
                n_secondaries_to_request = write_concern - 1
                s1_done, s2_done = threading.Event(), threading.Event()
                s1_thread = threading.Thread(
                    target=retry, args=(MasterConnectionHandler.replicate, secondaries_dns[0], request["msg"], local_transaction_id, s1_done), daemon=True)
                s2_thread = threading.Thread(
                    target=retry, args=(MasterConnectionHandler.replicate, secondaries_dns[1], request["msg"], local_transaction_id, s2_done), daemon=True)
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

            global log
            log[local_transaction_id] = request['msg']
            response = f'Appended {local_transaction_id:02} "{request["msg"]}" to the log list\n'

            self.send_response(200)
            self.end_headers()
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
    def replicate(secondary_dns, msg, transaction_id, resp_received_event):
        try:
            print(f'Sending replicate request for "{msg}" message to {secondary_dns}')
            # c = HTTPConnection(secondary_dns, secondary_port) # blocking replication -> no timeout
            c = HTTPConnection(secondary_dns, secondary_port, timeout=0.1)
            c.request('POST', '/replicate', body=f'{{"msg": "{msg}", "transaction_id": "{transaction_id}"}}')
            status = c.getresponse().status # waits for timeout seconds
            print(f'Got {"ACK" if status == 200 else "NACK"} trying to replicate {(transaction_id, msg)} to {secondary_dns}')
            resp_received_event.set()
        except ConnectionRefusedError as e:
            print(f'{secondary_dns} is down')
            raise e

class SecondaryConnectionHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/rejoin':
            self.send_response(200)
            self.end_headers()
            response = json.dumps(log)
            self.wfile.write(response.encode())
            return
        else:
            self.send_response(404)
            self.end_headers()
            response = 'Not found\n'
            self.wfile.write(response.encode())
            return

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""

class Master:
    def __init__(self, my_dns, client_facing_port, secondaries_facing_port):
        self.my_dns = my_dns
        self.client_facing_port = client_facing_port
        self.secondaries_facing_port = secondaries_facing_port

    def start(self):
        client_facing_server_thread = threading.Thread(target=Master.startClientFacingServer, args=(self,))
        secondaries_facing_thread = threading.Thread(target=Master.startSecondariesFacingServer, args=(self,))
        client_facing_server_thread.start()
        secondaries_facing_thread.start()
        client_facing_server_thread.join()
        secondaries_facing_thread.join()

    def startClientFacingServer(self):
        print('log2', self)
        http_server = ThreadedHTTPServer((self.my_dns, self.client_facing_port), MasterConnectionHandler)
        print('Starting client facing http server, use <Ctrl-C> to stop')
        http_server.serve_forever()

    def startSecondariesFacingServer(self):
        print('log1', self)
        http_server = ThreadedHTTPServer((self.my_dns, self.secondaries_facing_port), SecondaryConnectionHandler)
        print('Starting secondaries facing http server, use <Ctrl-C> to stop')
        http_server.serve_forever()

master = Master('Master', 8080, 8081)
master.start()