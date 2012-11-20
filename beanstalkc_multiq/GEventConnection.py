__author__ = 'jon'

import beanstalkc
import gevent

# creates a beanstalkc  connection, or raises an exception
def __connect__(host, port):
    return beanstalkc.Connection(host=host, port=port)

def __reserve__(wrapper, tube):
    # will create the connection if required
    conn = wrapper.reader()
    conn.watch(tube)
    job = conn.reserve()
    return job

class ConnectionWrapper:

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.reader_conn = None
        self.writer_conn = None

    def reader(self):
        if self.reader_conn:
            return self.reader_conn

        # raises an exception if a connection can't be established
        g = gevent.spawn(__connect__, self.host, self.port)

        gevent.joinall([g])
        self.reader_conn = g.value
        return self.reader_conn

## NOTE!! it's required you monkey patch the sockets to work with gevent, otherwise this
## will not work properly
class GEventConnection:

    def __init__(self, server_list=[beanstalkc.DEFAULT_HOST]):
        self.server_list = self.__parse_servers__(server_list)
        self.connections = self.__connect__()

    def __parse_servers__(self, server_list):
        parsed_list = []
        for server in server_list:
            parts = server.split(":")
            host = parts[0]
            port = beanstalkc.DEFAULT_PORT
            if len(parts) > 1 and len(parts) < 3:
                port = parts[1]
            port = int(port)
            parsed_list.append( (host,port) )
        return parsed_list

    def __listen_for_jobs__(self, wrapper, tube, callback):
        print "Rescheduling listen on tube %s" % tube

        # spawn the listen job to listen
        g = ReserveGreenlet(self, wrapper, tube, callback)
        g.link_value(pass_value(g.success_callback))
        g.start_later(0)

    # listens for jobs on the particular tube on all servers.  This is a non-blocking call.
    def listen(self, tube, callback):
        # we should lock the tube (another call to listen is bad)
        for wrapper in self.connections:
            conn = wrapper.reader()
            conn.watch(tube)
            self.__listen_for_jobs__(wrapper, tube, callback)

    # attempts to connect to the servers
    def __connect__(self):
        wrappers = []
        for server_entry in self.server_list:
            wrapper = ConnectionWrapper(server_entry[0], server_entry[1])
            wrappers.append(wrapper)
        return wrappers

class ReserveGreenlet(gevent.Greenlet):

    def __init__(self, conn, wrapper, tube, callback):
        gevent.Greenlet.__init__(self)
        self.conn = conn
        self.wrapper = wrapper
        self.tube = tube
        self.cb = callback

    def _run(self):
        return __reserve__(self.wrapper, self.tube)
        # do i need to call gevent.sleep(0) here?

    def success_callback(self, job):
        self.cb(job)
        # re-schedule the job
        gevent.spawn_later(0, self.conn.__listen_for_jobs__, self.wrapper, self.tube, self.cb)

# helper to pass the value to the callback
class pass_value(object):
    __slots__ = ['callback']

    def __init__(self, callback):
        self.callback = callback

    def __call__(self, source):
        if source.successful():
            self.callback(source.value)

    def __hash__(self):
        return hash(self.callback)

    def __eq__(self, other):
        return self.callback == getattr(other, 'callback', other)

    def __str__(self):
        return str(self.callback)

    def __repr__(self):
        return repr(self.callback)

    def __getattr__(self, item):
        assert item != 'callback'
        return getattr(self.callback, item)
