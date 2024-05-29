#!/usr/local/bin/python3
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
import threading

import json
#remove:
import time

log = []

class ClientConnectionHandler(BaseHTTPRequestHandler):
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

class MasterConnectionHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        # print(self.headers)
        if self.path == '/replicate':
            #input("input1") # block for testing delays

            content_len = int(self.headers.get('Content-Length'))
            request = json.loads(self.rfile.read(content_len))
            print(f'Received replicate request for "{request["msg"]}" message from master')

            self.send_response(200)
            self.end_headers()
            # input("input2")

            global log
            log.append(request["msg"])

            response = f'Appended "{request["msg"]}" to the log list\n'
            self.wfile.write(response.encode())
            print(response, end='')

            # input("input3")
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
    def __init__(self, my_dns, client_facing_port, master_facing_port):
        self.my_dns = my_dns
        self.client_facing_port = client_facing_port
        self.master_facing_port = master_facing_port
        # self.master_facing_ip = master_facing_ip
        
    def start(self):
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
    master_facing_port=8081)
    # master_facing_ip='Master',
master.start()