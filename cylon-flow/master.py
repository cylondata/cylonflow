import zmq
import time
import random


def server_pub(port="5558"):
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://*:%s" % port)
    publisher_id = random.randrange(0, 9999)
    print("Running server on port: ", port)
    # serves only 5 request and dies
    time.sleep(2)
    for reqnum in range(10):
        # Wait for next request from client
        topic = random.randrange(8, 10)
        # messagedata = "%d server#%s" % (reqnum, publisher_id)
        messagedata = f"print(\"rank\",comm.Get_rank())"
        print("%s %s" % (topic, messagedata))
        socket.send_string("%d %s" % (topic, messagedata))
        time.sleep(1)


server_pub()
