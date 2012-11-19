from threading import Thread
import gevent


__author__ = 'jon'

import Connection

def callback(job):
    print "I was called with a job!"
    job.delete()

conn = Connection.Connection(['localhost', 'localhost:5566'])

conn.listen("pushit-notif", callback)

# gets blocked here

#print("Put data on the queue?")

while True:
    gevent.sleep(5.0)
    conn.put("pushit-notif", "some data to process")