import grpc
from protos import banking_system_pb2 as pb2
from protos import banking_system_pb2_grpc as pb2_grpc
from concurrent import futures
import time
import sys
import json
from multiprocessing import Process


class Branch(pb2_grpc.exchange_messagesServicer):
    def __init__(self, id, balance, branches):
        self.id = id
        self.balance = balance
        self.branches = [branch["id"] for branch in branches if branch["type"] == "branch"]
        self.stubList = list()
        self.branch_events = []  # List to store branch events
        self.logical_clock = 0  # Initialize logical clock
        self.stub = None
        self.init_stub()

    def init_stub(self):
        for branch in self.branches:
            channel = grpc.insecure_channel(f'localhost:{50054 + branch}')
            stub = pb2_grpc.exchange_messagesStub(channel)
            if(branch == self.id):
                self.stub = stub
            self.stubList.append(stub)

    def update_logical_clock(self, received_logical_clock):
        self.logical_clock = max(self.logical_clock, received_logical_clock) + 1

    def track_event(self, request, breceiving_id, interface):
        comment = ''
        if (breceiving_id == -1):
            if (interface not in ['propagate_deposit', 'propagate_withdraw']):
                comment += 'event_recv from customer ' + str(request.id)
            else:
                comment += 'event_recv from branch ' + str(request.id)
        else:
            comment += 'event_sent to branch ' + str(breceiving_id)
        self.branch_events.append({
            "customer-request-id": request.id,
            "logical_clock": self.logical_clock,
            "interface": interface,
            "comment": comment
        })

    def construct_res(self):
        return {'id': self.id, 'type': 'branch', 'events': self.get_branch_events()}

    def MsgDelivery(self, request, context):
        interface = request.interface
        # self.update_logical_clock(request.logical_timestamp)
        response = None
        if interface == "query":
            self.update_logical_clock(request.logical_timestamp)
            self.track_event(request, -1, interface='query')
            response = pb2.Response(interface="query", balance=self.balance, logical_timestamp=self.logical_clock)
            # return response

        elif interface == "deposit":
            money = request.money
            self.balance += money
            self.update_logical_clock(request.logical_timestamp)
            self.track_event(request, -1, interface='deposit')
            # Propagate deposit to other branches
            for receiving_id, stub in enumerate(self.stubList,1):
                if(stub == self.stub):
                    continue
                self.update_logical_clock(request.logical_timestamp)
                self.track_event(request, receiving_id, interface='propagate_deposit')
                stub.MsgDelivery(pb2.Request(id=self.id, interface="propagate_deposit", money=money,
                                             logical_timestamp=self.logical_clock))
            response = pb2.Response(interface="deposit", result="success", logical_timestamp=self.logical_clock)
            # return response

        elif interface == "withdraw":
            money = request.money
            self.update_logical_clock(request.logical_timestamp)
            self.track_event(request, -1, interface='withdraw')
            if self.balance >= money:
                self.balance -= money
                # Propagate withdraw to other branches
                for receiving_id, stub in enumerate(self.stubList,1):
                    if (stub == self.stub):
                        continue
                    self.update_logical_clock(request.logical_timestamp)
                    self.track_event(request, receiving_id, interface='propagate_withdraw')
                    stub.MsgDelivery(pb2.Request(id=self.id, interface="propagate_withdraw", money=money,
                                                 logical_timestamp=self.logical_clock))
                response = pb2.Response(interface=interface, result="success", logical_timestamp=self.logical_clock)
            else:
                response = pb2.Response(interface=interface, result="fail", logical_timestamp=self.logical_clock)
            # return response

        elif interface == "propagate_withdraw":
            money = request.money
            self.update_logical_clock(request.logical_timestamp)
            self.track_event(request, -1, interface='propagate_withdraw')
            if self.balance >= money:
                self.balance -= money
                response = pb2.Response(interface="propagate_withdraw", result="success",
                                        logical_timestamp=self.logical_clock)
            else:
                response = pb2.Response(interface="propagate_withdraw", result="fail",
                                        logical_timestamp=self.logical_clock)
            # return response

        elif interface == "propagate_deposit":
            money = request.money
            self.update_logical_clock(request.logical_timestamp)
            self.track_event(request, -1, interface='propagate_deposit')
            self.balance += money
            response = pb2.Response(interface="propagate_deposit", result="success",
                                    logical_timestamp=self.logical_clock)
            # return response
        tmp = sys.stdout
        sys.stdout = open('branch_file.txt', "a")
        print(self.construct_res())
        sys.stdout.close()
        sys.stdout = tmp
        return response

    def get_branch_events(self):
        return self.branch_events


def load_json(filename):
    with open(filename, 'r') as file:
        return json.load(file)


# Load input data (customers and branches)
input_data = load_json('input_10.json')


def run_branch(data):
    branch = Branch(data["id"], data["balance"], input_data)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_exchange_messagesServicer_to_server(branch, server)
    print(f"Branch stub for {data['id']} created at :" + 'localhost:' + str(50054 + data["id"]))
    server.add_insecure_port('[::]:' + str(50054 + data["id"]))
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    with open('branch_file.txt', 'w') as fp:
        pass

    branch_processes = [Process(target=run_branch, args=(data,)) for data in input_data if data["type"] == "branch"]

    # Start branch processes
    for process in branch_processes:
        process.start()
        time.sleep(2)

    # Wait for all processes to finish
    for process in branch_processes:
        process.join()
