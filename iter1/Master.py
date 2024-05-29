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
            self.send_response(200)
            self.end_headers()
            content_len = int(self.headers.get('Content-Length'))
            request = json.loads(self.rfile.read(content_len))

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                # Start the load operations and mark each future with its URL
                future_to_secondary = {executor.submit(MasterConnectionHandler.replicate, s, request["msg"]):s for s in secondaries_dns}
                for future in concurrent.futures.as_completed(future_to_secondary):
                    try:
                        response_status = 'ACK(status code 200)' if future.result() == 200 else 'NACK'
                        # todo check for 200 -> ACK
                        print(f'{future_to_secondary[future]} responded with {response_status}')
                    except Exception as exc:
                        print('%s generated an exception: %s' % (future_to_secondary[future], exc))

            # threading.Lock().acquire()
            global log
            log.append(request["msg"])
            # threading.Lock().release()

            response = f'Appended "{request["msg"]}" to the log list\n'
            self.wfile.write(response.encode())
            return
        else:
            self.send_response(404)
            self.end_headers()
            response = 'Not found\n'
            self.wfile.write(response.encode())
            return

    @staticmethod
    def replicate(secondary_dns, msg):
        print(f'Sending replicate request for "{msg}" message to {secondary_dns}')
        c = HTTPConnection(secondary_dns, secondary_port) # blocking replication -> no timeout
        # c = HTTPConnection(secondary_dns, secondary_port, timeout=2)
        # handle error?
        # if request blocks ?
        # retries ? -> could do here by catching timeout exceptions
        c.request('POST', '/replicate', body=f'{{"msg": "{msg}"}}')
        status = c.getresponse().status # waits for timeout seconds
        return status

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