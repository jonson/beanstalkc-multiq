from threading import Thread

# needed for the socket stuff in beanstalkc
from gevent import monkey; monkey.patch_all()
import gevent


__author__ = 'jon'

#import Connection
import GEventConnection

def callback(job):
    print "I was called with a job! jid=%d" % job.jid
    job.delete()

#conn = Connection.Connection(['localhost', 'localhost:5566'])
conn = GEventConnection.GEventConnection(['localhost', 'localhost:5566'])

conn.listen("pushit-notif", callback)

# gets blocked here

#print("Put data on the queue?")

while True:
    gevent.sleep(3.0)
    conn.put("pushit-notif", "some data to process")