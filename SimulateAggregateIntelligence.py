import random
import simpy
from typing import List
import logging


class Task:
    def __init__(self, task_id, num_operations, operation_range):
        self.task_id = task_id
        self.operations = random.sample(range(1, operation_range + 1), num_operations)


class WorkerNode:
    def __init__(self, env, node_id, worker_max_operations, operation_range):
        self.node_id = node_id
        self.operations = set()
        self.work_queue = simpy.Store(env)
        self.connected_nodes = []

    def connect_node(self, other_node):
        logging.info(f"Node {self.node_id} connecting to node {other_node.node_id}")
        self.connected_nodes.append(other_node)

    def add_task(self, task):
        logging.info(f"Node {self.node_id} accepting task {task.task_id}")
        self.work_queue.put(task)

    def add_operation(self, operation):
        self.operations.add(operation)

    def process_task(self):
        while True:
            task = yield self.work_queue.get()
            logging.info(f"Node {self.node_id} processing task {task.task_id} requiring operations {task.operations}")
            if self.can_perform_operations(task):
                operation = self.get_matching_operation(task)
                logging.info(f"Node {self.node_id} performing operation {operation} on task {task.task_id}")
                self.perform_operation(operation, task)
            else:
                logging.info(f"Node {self.node_id} forwarding task {task.task_id}")
                self.forward_task(task)

    def can_perform_operations(self, task):
        return any(operation in task.operations for operation in self.operations)

    def get_matching_operation(self, task):
        return random.choice(list(set(self.operations) & set(task.operations)))

    def perform_operation(self, operation, task):
        print(f"Worker Node {self.node_id} performing operation {operation} for Task {task.task_id}")
        task.operations.remove(operation)
        env.timeout(task_processing_time)
        if not task.operations:
            print(f"Task {task.task_id} completed!")
            # TODO: record information for tracking
        else:
            # Requeue it for more work
            self.add_task(task)

    def forward_task(self, task):
        env.timeout(task_transfer_time)
        network.route(self, task)


class RandomNetwork:
    # Setup network of random connections with num_input_nodes input nodes
    @staticmethod
    def setup_network(worker_nodes: List[WorkerNode]) -> List[WorkerNode]:
        # connect the nodes in a random mesh
        for node in worker_nodes:
            connections_needed = worker_max_connections - len(node.connected_nodes)
            if connections_needed > 0:
                possible_connections = list(set(worker_nodes) - set(node.connected_nodes) - {node})
                [RandomNetwork.connect_nodes(node, new_connection) for new_connection
                 in random.sample(possible_connections, connections_needed)]
        return random.sample(worker_nodes, num_input_nodes)

    # Bidirectionally connect nodes
    @staticmethod
    def connect_nodes(node: WorkerNode, new_connection: WorkerNode):
        node.connect_node(new_connection)
        new_connection.connect_node(node)

    @staticmethod
    def route(from_node, task):
        # Send to a random recipient
        random.choice(from_node.connected_nodes).add_task(task)


def generate_tasks(env: simpy.Environment, target_nodes: List[WorkerNode]):
    task_id = 0
    while True:
        # Generate a task with a small set of randomly chosen process steps
        task = Task(task_id, worker_max_operations, operation_range)
        print(f"Task generated: {task}")

        # Assign the task to a random worker node for processing
        random.choice(target_nodes).add_task(task)
        task_id += 1

        # Simulate the time between task generation
        yield env.timeout(random.expovariate(task_generation_rate))


# Assign each node the ability to perform specific operations
def assign_skills(worker_nodes: List[WorkerNode]) -> None:
    # Insure each skill is assigned once
    for operation in range(1, operation_range):
        random.choice(worker_nodes).add_operation(operation)
    # Insure each worker has at least one skill
    for node in worker_nodes:
        if len(node.operations) == 0:
            node.add_operation(random.randint(1, operation_range))
    # Add extra operations so on average each worker has worker_max_operations
    for counter in range(operation_range, num_nodes * worker_max_operations):
        random.choice(worker_nodes).add_operation(random.randint(1, operation_range))
        counter += 1
    for node in worker_nodes:
        logging.info(f"Worker {node.node_id} can perform operations {node.operations}")


# Configuration variables
num_nodes = 5
num_input_nodes = 5
num_steps = 10
task_generation_rate = 0.5  # Tasks per time unit
worker_max_operations = 3
operation_range = 10
worker_max_connections = 2
random.seed(42)
task_processing_time = 2
task_transfer_time = 1

# Create SimPy environment and initialize worker nodes
logging.basicConfig(level=logging.INFO)
env = simpy.Environment()
network = RandomNetwork()

nodes = [WorkerNode(env, node_id, worker_max_operations, operation_range)
         for node_id in range(num_nodes)]

assign_skills(nodes)

# Set up the network connections
entry_nodes = RandomNetwork.setup_network(nodes)
logging.info("Entry nodes are {}" % node.node_id for node in entry_nodes)

# Start the worker processes
[env.process(node.process_task()) for node in nodes]

# Start the task generator process
env.process(generate_tasks(env, entry_nodes))

# Run the simulation for a specific duration
sim_duration = 10  # Simulation duration in time units
env.run(until=sim_duration)
