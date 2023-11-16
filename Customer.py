import os

import grpc
from protos import banking_system_pb2 as pb2
from protos import banking_system_pb2_grpc as pb2_grpc
import json
import sys
from multiprocessing import Process
import time


class Customer:
    def __init__(self, id, events):
        self.id = id
        self.events = events
        self.recvMsg = list()
        self.stub = None
        self.logical_clock = 0  # Initialize logical clock

    def createStub(self):
        channel = grpc.insecure_channel('localhost:' + str(50054 + self.id))
        self.stub = pb2_grpc.exchange_messagesStub(channel)

    def update_logical_clock(self, received_logical_clock):
        self.logical_clock = max(self.logical_clock, received_logical_clock) + 1

    def executeEvents(self):
        customer_events = []  # List to store customer events
        customer2_events = []
        res = []
        res2 = []
        for event in self.events:
            interface = event["interface"]
            customer_request_id = event["customer-request-id"]

            # Increment logical clock on each event
            self.logical_clock += 1

            # Create event dictionary
            event_dict = {
                "customer-request-id": customer_request_id,
                "logical_clock": self.logical_clock,
                "interface": interface,
                "comment": f"event_sent from customer {self.id}"
            }

            customer_events.append(event_dict)

            event_dict2 = {
                "customer-request-id": customer_request_id,
                "logical_clock": self.logical_clock,
                "interface": interface,
                "comment": f"event_sent to branch {self.id}"
            }

            customer2_events.append(event_dict2)

            if interface == "query":
                request = pb2.Request(id=self.id, interface=interface, logical_timestamp=self.logical_clock)
                response = self.stub.MsgDelivery(request)
                self.update_logical_clock(response.logical_timestamp)
                self.recvMsg.append({"interface": interface, "result": response.balance,
                                     "logical_timestamp": response.logical_timestamp})

            elif interface == "deposit":
                money = event["money"]
                request = pb2.Request(id=self.id, interface=interface, money=money,
                                      logical_timestamp=self.logical_clock)
                response = self.stub.MsgDelivery(request)
                self.update_logical_clock(response.logical_timestamp)
                self.recvMsg.append({"interface": interface, "result": response.result,
                                     "logical_timestamp": response.logical_timestamp})

            elif interface == "withdraw":
                money = event["money"]
                request = pb2.Request(id=self.id, interface=interface, money=money,
                                      logical_timestamp=self.logical_clock)
                response = self.stub.MsgDelivery(request)
                self.update_logical_clock(response.logical_timestamp)
                self.recvMsg.append({"interface": interface, "result": response.result,
                                     "logical_timestamp": response.logical_timestamp})
        # Print customer events
        # print(f"Customer {self.id} events:")
        # for e in customer_events:
        #     print(e)
        # print(f"Customer {self.id} received message: {self.recvMsg}")
        res.append({'id': self.id, 'type': 'customer', 'events': customer_events})
        tmp = sys.stdout
        sys.stdout = open('customer_file.txt', "a")
        print(res)
        sys.stdout.close()
        sys.stdout = tmp

        res2.append({'id': self.id, 'type': 'customer', 'events': customer2_events})
        tmp = sys.stdout
        sys.stdout = open('event_file.txt', "a")
        print(res2)
        sys.stdout.close()
        sys.stdout = tmp


def load_json(filename):
    with open(filename, 'r') as file:
        return json.load(file)


# Load input data (customers and branches)
input_data = load_json('input_10.json')


def run_customer(data):
    customer = Customer(data["id"], data["customer-requests"])
    customer.createStub()
    customer.executeEvents()


if __name__ == '__main__':
    # Create separate processes for customers
    with open('customer_file.txt', 'w') as fp:
        pass

    with open('event_file.txt', 'w') as fp:
        pass

    customer_processes = [Process(target=run_customer, args=(data,)) for data in input_data if
                          data["type"] == "customer"]

    # Start customer processes
    for process in customer_processes:
        process.start()
        # time.sleep(2)

    # Wait for all processes to finish
    for process in customer_processes:
        process.join()

    # Read Customer data and convert to JSON format
    with open('customer_file.txt', 'r') as cf:
        customer = []
        for line in cf:
            cust = eval(line)[0]
            customer.append(cust)

    # Convert 'customer' list to JSON format
    json_data = json.dumps(customer, indent=4)

    # Write JSON data to file
    with open('customer_output.json', 'w') as json_file:
        json_file.write(json_data)

    # Read Events data and convert to JSON format
    with open('event_file.txt', 'r') as ef:
        event = []
        for line in ef:
            eve = eval(line)[0]
            event.append(eve)

    # Convert 'event' list to JSON format
    json_data = json.dumps(event, indent=4)

    # Write JSON data to file
    with open('event_output.json', 'w') as ef:
        ef.write(json_data)

    # Read Branch data and convert to json format
    with open('branch_file.txt') as bf:
        branch = []
        for line in bf:
            bnch = eval(line)
            branch.append(bnch)

    # Remove redundant data
    merged_data = {}

    for entry in branch:
        id = entry["id"]
        if id not in merged_data:
            merged_data[id] = entry
            merged_data[id]["events"] = []
        merged_data[id]["events"].extend(event for event in entry["events"] if event not in merged_data[id]["events"])

    merged_data_list = list(merged_data.values())

    # Convert 'branch' list to JSON format
    json_data = json.dumps(merged_data_list, indent=4)

    # Write JSON data to file
    with open('branch_output.json', 'w') as file:
        file.write(json_data)

    # Read customer and branch events
    with open('branch_output.json', 'r') as bf, open('event_output.json', 'r') as ef:
        branch_data = json.load(bf)
        event_data = json.load(ef)


    def convert_event(event, entry_id):
        '''CONVERT JSON TO REQUIRED FORMAT'''
        return {
            "id": entry_id,
            "customer-request-id": event["customer-request-id"],
            "type": entry["type"],
            "logical_clock": event["logical_clock"],
            "interface": event["interface"],
            "comment": event["comment"]
        }


    # Merge customer and branch event
    merged_data = []
    for entry in event_data + branch_data:
        entry_id = entry["id"]
        entry_type = entry["type"]
        events = entry["events"]
        for event in events:
            converted_event = convert_event(event, entry_id)
            merged_data.append(converted_event)

    # Write JSON data to file
    with open('event_output.json', 'w') as mf:
        json.dump(merged_data, mf, indent=4)
    print('output files are generated:\ncustomer_output.json\nbranch_output.json\nevent_output.json')
    os.remove('customer_file.txt')
    os.remove('branch_file.txt')
    os.remove('event_file.txt')
