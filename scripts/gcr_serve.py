#!/usr/bin/env python

from http.server import HTTPServer, BaseHTTPRequestHandler
import os
import sys
import subprocess
import requests


class S(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            subprocess.run(sys.argv[1:], env=os.environ.copy())
        except:
            pass

        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write("OK".encode("utf8"))
        sys.exit(1)


if __name__ == "__main__":
    httpd = HTTPServer(("", int(os.environ["PORT"])), S)
    httpd.serve_forever()
