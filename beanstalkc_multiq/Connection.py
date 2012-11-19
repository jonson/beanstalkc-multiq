
import threading
import math
import beanstalkc

from gevent import monkey; monkey.patch_all()
import geventreactor; geventreactor.install()

from twisted.internet import threads
from twisted.internet import reactor, defer

class BeanstalkManager(threading.Thread):
    def run(self):
        print "Running reactor"
        reactor.run(installSignalHandlers=0)

# creates a beanstalkc  connection, or raises an exception
def __connect__(host, port):
    return beanstalkc.Connection(host=host, port=port)

def __reserve__(wrapper, tube):
    # will create the connection if required
    conn = wrapper.reader()
    conn.watch(tube)
    job = conn.reserve()
    return job

# internal connection wrapper
class ConnectionWrapper:

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.reader_conn = None
        self.writer_conn = None
        self.reader_lock = threading.Lock()
        self.writer_lock = threading.Lock()
        self.reader_retries = 0

    ## the reader connection, only to be used for reading data.
    def reader(self):
        if self.reader_conn:
            return self.reader_conn

        with self.reader_lock:
            print "Creating reader connection for %s:%d" % (self.host, self.port)
            self.reader_conn = __connect__(self.host, self.port)

        return self.reader_conn

    ## the writer connection, will be used to send data to beanstalk
    def writer(self):
        if self.writer_conn:
            return self.writer_conn

        with self.writer_lock:
            print "Creating writer connection for %s:%d" % (self.host, self.port)
            self.writer_conn = __connect__(self.host, self.port)

        return self.writer_conn

    def destroy_reader(self):
        if self.reader_conn:
            self.reader_conn.close()
            self.reader_conn = None

    def destroy_writer(self):
        if self.writer_conn:
            self.writer_conn.close()
            self.writer_conn = None


## Multi-server connection capable of both reading and writing at the same time.  It actually
## creates two connections (reader,writer) for each host listed.
class Connection:

    def __init__(self, server_list=[beanstalkc.DEFAULT_HOST]):
        self.server_list = self.__parse_servers__(server_list)

        self.connections = self.__connect__()

        self.reactor_thread = BeanstalkManager()
        self.reactor_thread.start()

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

    def __reader_error_handler__(self, err, wrapper, tube, callback):
        # nuke this connection
        wrapper.destroy_reader()

        # re-schedule it
        pow = min(wrapper.reader_retries, 4)
        delay = math.pow(2, pow)
        wrapper.reader_retries += 1

        print("Error, trying to reschedule listener in %d seconds" % delay)

        reactor.callLater(delay, self.__listen_for_jobs__, wrapper, tube, callback)

    def __reschedule_callback__(self, job, wrapper, tube, callback):
        callback(job)
        reactor.callLater(0, self.__listen_for_jobs__, wrapper, tube, callback)

    def __listen_for_jobs__(self, wrapper, tube, callback):
        print "Rescheduling listen on tube %s" % tube
        d = threads.deferToThread(__reserve__, wrapper, tube)

        args = (wrapper, tube, callback)
        d.addCallbacks(self.__reschedule_callback__, self.__reader_error_handler__, callbackArgs=args, errbackArgs=args)


    # listens for jobs on the particular tube on all servers.  This is a non-blocking call.
    def listen(self, tube, callback):
        # we should lock the tube (another call to listen is bad)
        for wrapper in self.connections:
            conn = wrapper.reader()
            conn.watch(tube)
            self.__listen_for_jobs__(wrapper, tube, callback)

    # attempts to put the job on one of the queues.  this is a blocking call, it will attempt to place
    # it on the queues in the order specified in creation.
    # this is an exact copy of the beanstalkc put method
    def put(self, tube, job, priority=beanstalkc.DEFAULT_PRIORITY, delay=0, ttr=beanstalkc.DEFAULT_TTR):

        err = None

        for wrapper in self.connections:
            try:
                conn = wrapper.writer()
                conn.use(tube)
                print("attempting to put job on the queue")
                res = conn.put(job, priority, delay, ttr)
                err = None
                break
            except Exception, e:
                # an error putting something on the queue, nuke the writer, will be re-created next call
                conn = wrapper.destroy_writer()
                print "Error!: %s" % e
                err = e

        # if error was not cleared, raise it to the caller
        if err:
            raise err

    # attempts to connect to the servers
    def __connect__(self):
        wrappers = []
        for server_entry in self.server_list:
            wrapper = ConnectionWrapper(server_entry[0], server_entry[1])
            wrappers.append(wrapper)
        return wrappers