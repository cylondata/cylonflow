import zmq
from mpi4py import MPI

comm = MPI.COMM_WORLD


# rank = comm.Get_rank()


# def client(port_push, port_sub):
def client(port_sub):
    context = zmq.Context()
    # socket_pull = context.socket(zmq.PULL)
    # socket_pull.connect("tcp://localhost:%s" % port_push)
    # print("Connected to server with port %s" % port_push)
    socket_sub = context.socket(zmq.SUB)
    socket_sub.connect("tcp://localhost:%s" % port_sub)
    socket_sub.setsockopt_string(zmq.SUBSCRIBE, "")
    # socket_sub.setsockopt(zmq.LINGER, -1)
    print("Connected to publisher with port %s" % port_sub)
    # Initialize poll set
    poller = zmq.Poller()
    # poller.register(socket_pull, zmq.POLLIN)
    poller.register(socket_sub, zmq.POLLIN)

    # Work on requests from both server and publisher

    should_continue = True
    while should_continue:
        socks = dict(poller.poll())
        # if socket_pull in socks and socks[socket_pull] == zmq.POLLIN:
        #     message = socket_pull.recv()
        #     print("Recieved control command: %s" % message)
        #     if message == "Exit":
        #         print("Recieved exit command, client will stop recieving messages")
        #         should_continue = False

        # if socket_sub in socks and socks[socket_sub] == zmq.POLLIN:
        if socket_sub in socks:
            string = socket_sub.recv()
            topic, messagedata = string.split()
            print("Processing ... ", topic, messagedata)
            exec(messagedata, {'comm': comm})


client("5558")
