#!/usr/bin/python

# MogileFS Proxy Server (Moxie)
# Forwards tracker requests to one of two sets of trackers.  The net effect is that
# one set acts as a write cache/local store, the other as a fall through read store.
# Writes go to one set, whereas reads are attempted on the cache first, then
# proceed to the second set on failure.  This is useful if one would like to have
# a development or staging environment be able to write, yet have read only access
# to a production environment (perhaps accessible via vpn).
# @author Dan Kuebrich

# Copyright (c) 2009, Amie Street, Inc.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of Amie Street, Inc. nor the names of its
#      contributors may be used to endorse or promote products derived from
#      this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
# IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
# NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

import getopt
import socket
import sys
import threading

# trackers_a is the write/cache store
# trackers_b is the read store
config = {  'localport' : 6002,
            'trackers_a' : [ 'mogiletracker.corp.whatever.com:6001' ],
            'trackers_b' : [ 'mogiletracker1.prod.whatever.com:6001',
                             'mogiletracker2.prod.whatever.com:6001' ] }

# debug levels
MOXIE_ERROR = 5
MOXIE_DEBUG = 1


class MThread (threading.Thread):
    def __init__(self, insock, debug_level):
        threading.Thread.__init__(self)
        self.s = insock
        self.debug_level = debug_level

    def run(self):
        c_if = self.s.makefile("r")
        c_of = self.s.makefile("w")

        req = c_if.readline()
        t_sock = self.get_tracker_socket('trackers_a')
        if not t_sock:
            self.debug(MOXIE_ERROR, "TRACKER DOWN in trackers_a: %s" % str(config['trackers_a']))
            return

        r_sock = self.get_tracker_socket('trackers_b')
        if not r_sock:
            self.debug(MOXIE_ERROR, "TRACKER DOWN in trackers_b: %s" % str(config['trackers_b']))
            return

        while req:
            resp = self.forward_req(t_sock, req)
            s = resp.split()[:2]
            # none_match is included here so that listkey can return every key
            # from both sets of trackers
            if s[0] == 'ERR' and s[1] in set(['unknown_key','none_match']):
                resp = self.forward_req(r_sock, req)
            c_of.write(resp)
            c_of.flush()
            req = c_if.readline()

        self.s.close()

    def req_pass(self, req):
        if (req[0:6] == 'DELETE' or req[0:6] == 'RENAME'):
            return False
        return True

    def forward_req(self, t_sock, req):
        self.debug(MOXIE_DEBUG, "=> %s: %s" % (t_sock.getpeername()[0], req))
        t_of = t_sock.makefile("w")
        t_if = t_sock.makefile("r")
        t_of.write(req)
        t_of.flush()
        return t_if.readline()
    
    def get_tracker_socket(self, list):
        for t in config[list]:
            parts = t.split(":")
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((parts[0], int(parts[1])))
                return s
            except Exception, e:
                return False
        return False

    def debug(self, level, msg):
        if level >= self.debug_level:
            print >>sys.stderr, msg


def serve(debug_level = MOXIE_ERROR):
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serversocket.bind(('', config['localport']))
    serversocket.listen(5)

    while True:
        (csocket, address) = serversocket.accept()
        mt = MThread(csocket, debug_level)
        mt.start()


def main(argv = None):
    debug_level = MOXIE_ERROR

    if argv == None:
        argv = sys.argv

    # parse command line options
    try:
        opts, args = getopt.getopt(argv[1:], "hv", ["help","verbose"])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)
    # process options
    for o, a in opts:
        if o in ("-h", "--help"):
            print "Usage: moxie.py [-v/--verbose] [-h/--help]\nTrackers are configured in the source.\n"
            sys.exit(0)
        if o in ("-v", "--verbose"):
            debug_level = MOXIE_DEBUG
    serve(debug_level)


if __name__ == '__main__':
    main()
