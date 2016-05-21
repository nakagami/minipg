#!/usr/bin/env python3
##############################################################################
#The MIT License (MIT)
#
#Copyright (c) 2016 Hajime Nakagami
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.
##############################################################################
import sys
import socket
import binascii

def asc_dump(s):
    r = ''
    for c in s:
        r += chr(c) if (c >= 32 and c < 128) else '.'
    if r:
        print('\t[' + r + ']')


def parse_message(server_sock, client_sock):
    server_code = 0
    while server_code == 90:
        server_head = server_sock.recv(5)
        server_code = server_head[0]
        server_ln = int.from_bytes(server_head[1:], byteorder='big')
        server_data = server_sock.server(server_ln)
        print('<<', chr(server_code), binascii.b2a_hex(server_data), asc_dump(server_data))
        client_sock.send(server_head)
        client_sock.send(server_data)


def read_login_packet(sock):
    head = sock.recv(4)
    ln = int.from_bytes(head, byteorder='big')
    return head + sock.recv(ln-4)


def proxy_wire(server_name, server_port, listen_host, listen_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((listen_host, listen_port))
    sock.listen(1)
    client_sock, addr = sock.accept()
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.connect((server_name, server_port))

    login_packet = read_login_packet(client_sock)
    print('login:', binascii.b2a_hex(login_packet))
    parse_message(server_sock, client_sock)

    while True:
        client_head = client_sock.recv(5)
        client_code = client_head[0]
        client_ln = int.from_bytes(client_head[1:], byteorder='big')
        client_data = client_sock.recv(client_ln)
        print('>>', chr(client_code), binascii.b2a_hex(client_data), asc_dump(client_data))
        server_sock.send(client_head)
        server_sock.send(client_data)
        parse_message(server_sock, client_sock)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage : ' + sys.argv[0] + ' server[:port] [listen_host:]listen_port')
        sys.exit()

    server = sys.argv[1].split(':')
    server_name = server[0]
    if len(server) == 1:
        server_port = 5432
    else:
        server_port = int(server[1])

    listen = sys.argv[2].split(':')
    if len(listen) == 1:
        listen_host = 'localhost'
        listen_port = int(listen[0])
    else:
        listen_host = listen[0]
        listen_port = int(listen[1])

    proxy_wire(server_name, server_port, listen_host, listen_port)
