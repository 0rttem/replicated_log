#!/usr/local/bin/python3
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
import threading
from http.client import HTTPConnection

import json
from sortedcontainers import SortedDict
import time

# for total ordering
log = SortedDict()

def get_total_ordered(log):
    res = SortedDict()
    for i in range(len(log)):
        item = log.peekitem(i)
        if (item[0] != i):
            # could request rejoining here
            return res
        res[i] = item[1]
    return res

def retry(func, *args):
    try:
        func(*args)
    except TimeoutError as e:
        print(f'Will retry {func.__name__} with args={args} in 2s')
        time.sleep(2)
        retry(func, *args)

def rejoin(master_dns, master_port):
    c = HTTPConnection(master_dns, master_port, timeout=0.1)
    c.request('GET', '/rejoin')
    response = c.getresponse()
    status = response.status # waits for timeout seconds
    data = response.read()
    print(f'Rejoined Master. Got log ', data)
    global log
    log = SortedDict({int(k):v for k,v in json.loads(data).items()})

class ClientConnectionHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/list':
            self.send_response(200)
            self.end_headers()
            total_ordered_output = get_total_ordered(log)
            response = '\n'.join(map(lambda x: str(x), total_ordered_output.items())) + '\n'
            self.wfile.write(response.encode())
            return
        else:
            self.send_response(404)
            self.end_headers()
            response = 'Not found\n'
            self.wfile.write(response.encode())
            return

class MasterConnectionHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        # print(self.headers)
        if self.path == '/replicate':
            # input("input1") # block for testing delays

            content_len = int(self.headers.get('Content-Length'))
            request = json.loads(self.rfile.read(content_len))
            print(f'Received replicate request for {(int(request["transaction_id"]), request["msg"])} message from master')

            global log
            if int(request["transaction_id"]) in log:
                print(f'Duplicate message {(int(request["transaction_id"]), request["msg"])} received. Ignoring')
                self.send_response(200)
                self.end_headers()
                return

            log[int(request['transaction_id'])] = request['msg']

            response = f'Appended {int(request["transaction_id"]):02} "{request["msg"]}" to the log list\n'
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

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""

class Secondary:
    def __init__(self, my_dns, client_facing_port, master_facing_port, master_dns, master_port):
        self.my_dns = my_dns
        self.client_facing_port = client_facing_port
        self.master_facing_port = master_facing_port
        self.master_dns = master_dns
        self.master_port = master_port
        
    def start(self):
        retry(rejoin, self.master_dns, self.master_port)
        client_facing_server_thread = threading.Thread(target=Secondary.startClientFacingServer, args=(self,))
        master_facing_server_thread = threading.Thread(target=Secondary.startMasterFacingServer, args=(self,))
        client_facing_server_thread.start()
        master_facing_server_thread.start()
        client_facing_server_thread.join()
        master_facing_server_thread.join()

    def startClientFacingServer(self):
        http_server = ThreadedHTTPServer((self.my_dns, self.client_facing_port), ClientConnectionHandler)
        print('Starting client facing http server, use <Ctrl-C> to stop')
        http_server.serve_forever()

    def startMasterFacingServer(self):
        http_server = ThreadedHTTPServer((self.my_dns, self.master_facing_port), MasterConnectionHandler)
        print('Starting master facing http server, use <Ctrl-C> to stop')
        http_server.serve_forever()

master = Secondary(
    my_dns='Secondary2',
    client_facing_port=8080,
    master_facing_port=8081,
    master_dns='Master',
    master_port=8081)
master.start()