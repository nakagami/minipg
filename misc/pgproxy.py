#!/usr/bin/env python3
##############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2016 Hajime Nakagami
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
##############################################################################
import sys
import socket
import binascii


def recv_from_sock(sock, nbytes):
    n = nbytes
    recieved = b''
    while n:
        bs = sock.recv(n)
        recieved += bs
        n -= len(bs)
    return recieved


def asc_dump(s):
    r = ''
    for c in s:
        r += chr(c) if (c >= 32 and c < 128) else '.'
    if r:
        print('[' + r + ']')


def _recv_from_server(server_sock):
    server_head = recv_from_sock(server_sock, 5)
    server_code = server_head[0]
    server_ln = int.from_bytes(server_head[1:], byteorder='big') - 4
    server_data = recv_from_sock(server_sock, server_ln)
    print('S->C', chr(server_code), binascii.b2a_hex(server_data).decode('ascii'), end='')
    asc_dump(server_data)
    return server_head, server_data


def _recv_from_client(client_sock):
    client_head = recv_from_sock(client_sock, 5)
    client_code = client_head[0]
    client_ln = int.from_bytes(client_head[1:], byteorder='big')
    client_data = recv_from_sock(client_sock, client_ln-4)
    print('C->S', chr(client_code), binascii.b2a_hex(client_data).decode('ascii'), end='')
    asc_dump(client_data)
    return client_head, client_data


def _recv_from_client_tail(server_sock, client_sock):
    client_tail = recv_from_sock(client_sock, 5)
    assert client_tail == b'H\x00\x00\x00\x04'
    return client_tail


def parse_message(server_sock, client_sock):
    while True:
        server_head, server_data = _recv_from_server(server_sock)
        client_sock.send(server_head + server_data)
        if server_head[0] == 90:
            break


def read_login_packet(sock):
    head = recv_from_sock(sock, 4)
    ln = int.from_bytes(head, byteorder='big')
    return head + recv_from_sock(sock, ln-4)


def parse_login_response(server_sock, client_sock):
    server_head, server_data = _recv_from_server(server_sock)

    auth = int.from_bytes(server_data[:4], byteorder='big')
    if auth == 0:
        print('trust')
        client_sock.send(server_head + server_data)
    elif auth == 5:
        salt = server_data[4:]
        print('md5', binascii.b2a_hex(salt).decode('ascii'))
        client_sock.send(server_head + server_data)
        client_head, client_data = _recv_from_client(client_sock)
        server_sock.send(client_head + client_data)

        server_head, server_data = _recv_from_server(server_sock)
        # accept
        assert int.from_bytes(server_data[:4], byteorder='big') == 0
        client_sock.send(server_head + server_data)
    elif auth == 10:
        print('SASL')
        client_sock.send(server_head + server_data)
        client_head, client_data = _recv_from_client(client_sock)
        server_sock.send(client_head + client_data)

        server_head, server_data = _recv_from_server(server_sock)
        auth2 = int.from_bytes(server_data[:4], byteorder='big')
        assert auth2 == 11
        print('SCRAMFirst')
        client_sock.send(server_head + server_data)
        client_head, client_data = _recv_from_client(client_sock)
        server_sock.send(client_head + client_data)

        server_head, server_data = _recv_from_server(server_sock)
        auth3 = int.from_bytes(server_data[:4], byteorder='big')
        assert auth3 == 12
        print('SCRAMFinal')
        client_sock.send(server_head + server_data)

        server_head, server_data = _recv_from_server(server_sock)
        # accept
        assert int.from_bytes(server_data[:4], byteorder='big') == 0
        client_sock.send(server_head + server_data)


def proxy_wire(server_name, server_port, listen_host, listen_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((listen_host, listen_port))
    sock.listen(1)
    client_sock, addr = sock.accept()
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.connect((server_name, server_port))

    login_packet = read_login_packet(client_sock)
    print('C->S login ', binascii.b2a_hex(login_packet).decode('ascii'), end='')
    asc_dump(login_packet)
    server_sock.send(login_packet)
    parse_login_response(server_sock, client_sock)

    parse_message(server_sock, client_sock)

    while True:
        client_head, client_data = _recv_from_client(client_sock)
        server_sock.send(client_head + client_data)
        if chr(client_head[0]) == 'X':
            break
        _recv_from_client_tail(server_sock, client_sock)

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
