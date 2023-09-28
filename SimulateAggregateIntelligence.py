import json
import random
from dataclasses import dataclass, fields
import simpy
from typing import List
import logging
import pandas as pd
import argparse
import os
from abc import ABC, abstractmethod


# Configuration variables
@dataclass
class Config:
    num_nodes: int  # Number of nodes in the network
    num_input_nodes: int  # Number of nodes where work enters
    task_generation_rate: float  # Tasks generated per time unit
    worker_avg_operations: int  # Number of operations a worker can perform on average
    operation_range: int  # Size of bucket from which possible task operations are drawn
    worker_max_connections: int  # Max connections a worker can have to other workers
    task_processing_time: int  # Time units it takes a worker to process a task
    task_transfer_time: int  # Time units is takes a worker to transfer a task
    seed: int  # Seed to start random number generator (set to 0 to turn on 0 random behavior)
    sample_rate: int  # Period in (time units) to sample tasks done and outstanding
    num_runs: int  # Number of experimental runs
    sim_duration: int  # Number of time units to run the simulation for
    network: str  # 'random' or 'full-connect'


class Task:
    def __init__(self, config: Config, task_id, start_time):
        self.task_id = task_id
        self.operations = random.sample(range(1, config.operation_range + 1), config.worker_avg_operations)
        self.original_num_operations = len(self.operations)
        self.start_time = start_time
        self.config = config

    @property
    def complexity(self):
        return self.original_num_operations


class WorkerNode:
    def __init__(self, config: Config, env, node_id):
        self.config = config
        self.env = env
        self.node_id = node_id
        self.operations = set()
        self.work_queue = simpy.Store(self.env)
        self.connected_nodes = []
        self._tasks_completed = 0
        self.complexity_completed = 0
        self.tasks_awaiting_forwarding = 0
        self.tasks_in_process = 0

    def connect_node(self, other_node):
        logging.info(f"Worker {self.node_id} connecting to worker {other_node.node_id}")
        if other_node not in self.connected_nodes:
            self.connected_nodes.append(other_node)

    def add_task(self, task):
        log(f"Worker {self.node_id} accepting task {task.task_id}")
        self.work_queue.put(task)

    @property
    def wip(self):
        return len(self.work_queue.items) + self.tasks_awaiting_forwarding + self.tasks_in_process

    @property
    def tasks_completed(self):
        return self._tasks_completed

    @tasks_completed.setter
    def tasks_completed(self, value):
        self._tasks_completed = value

    def add_operation(self, operation):
        self.operations.add(operation)

    def process_task(self):
        while True:
            task = yield self.work_queue.get()
            log(f"Worker {self.node_id} processing task {task.task_id} requiring operations {task.operations}")
            if self.can_perform_operations(task):
                operation = self.get_matching_operation(task)
                self.tasks_in_process += 1
                log(f"Worker {self.node_id} performing operation {operation} on task {task.task_id}")
                task.operations.remove(operation)
                yield self.env.timeout(self.config.task_processing_time)
                self.tasks_in_process -= 1
                if not task.operations:
                    log(f"Task {task.task_id} completed!")
                    self.tasks_completed += 1
                    self.complexity_completed += task.complexity
                    record_task_completion(self.env.now, task)
                else:
                    self.add_task(task)  # Requeue it for more work
            else:
                self.tasks_awaiting_forwarding += 1
                log(f"Worker {self.node_id} forwarding task {task.task_id}")
                yield self.env.timeout(self.config.task_transfer_time)
                self.tasks_awaiting_forwarding -= 1
                network.route(self, task)

    def can_perform_operations(self, task):
        return any(operation in task.operations for operation in self.operations)

    def get_matching_operation(self, task):
        return random.choice(list(set(self.operations) & set(task.operations)))


# Abstract base class for a network implementation
class Network(ABC):
    # Set up the network configuration
    @staticmethod
    @abstractmethod
    def setup_network(config: Config, worker_nodes: List[WorkerNode]) -> List[WorkerNode]:
        pass

    # Route work from a node to a recipient (using whatever rules this network uses)
    @staticmethod
    def route(from_node, task):
        pass

    # Helper function to connect nodes bidirectionally
    @staticmethod
    def connect_nodes(node: WorkerNode, new_connection: WorkerNode):
        node.connect_node(new_connection)
        new_connection.connect_node(node)


class FullyConnectedNetwork(Network):
    # Setup network of num_input_nodes input nodes that are fully connected (each with every other)
    @staticmethod
    def setup_network(config: Config, worker_nodes: List[WorkerNode]) -> List[WorkerNode]:
        # Fully connect to all other nodes
        for node in worker_nodes:
            other_nodes = list(worker_nodes)
            other_nodes.remove(node)
            for other_node in other_nodes:
                node.connect_node(other_node)
        return worker_nodes # All nodes are inputs

    @staticmethod
    def route(from_node, task):
        # Send to a random recipient
        random.choice(from_node.connected_nodes).add_task(task)


class RandomNetwork(Network):
    # Setup network of random connections with num_input_nodes input nodes
    @staticmethod
    def setup_network(config: Config, worker_nodes: List[WorkerNode]) -> List[WorkerNode]:
        # connect the nodes in a random mesh
        for node in worker_nodes:
            connections_needed = config.worker_max_connections - len(node.connected_nodes)
            if connections_needed > 0:
                possible_connections = list(set(worker_nodes) - set(node.connected_nodes) - {node})
                [RandomNetwork.connect_nodes(node, new_connection) for new_connection
                 in random.sample(possible_connections, connections_needed)]
        return random.sample(worker_nodes, config.num_input_nodes)

    @staticmethod
    def route(from_node, task):
        # Send to a random recipient
        random.choice(from_node.connected_nodes).add_task(task)


def generate_tasks(config: Config, env: simpy.Environment, target_nodes: List[WorkerNode]):
    task_id = 0
    while True:
        # Generate a task with a small set of randomly chosen process steps
        task = Task(config, task_id, env.now)
        log(f"Generated task {task.task_id} with complexity {task.complexity} requiring operations {task.operations}")

        # Assign the task to a random worker node for processing
        random.choice(target_nodes).add_task(task)
        task_id += 1

        # Generate tasks at a constant time
        yield env.timeout(config.task_generation_rate)


# Assign each node the ability to perform specific operations
def assign_skills(config: Config, worker_nodes: List[WorkerNode]) -> None:
    # Insure each skill is assigned once
    for operation in range(1, config.operation_range + 1):
        random.choice(worker_nodes).add_operation(operation)
    # Insure each worker has at least one skill
    for node in worker_nodes:
        if len(node.operations) == 0:
            node.add_operation(random.randint(1, config.operation_range))
    # Add extra operations so on average each worker has worker_max_operations
    for counter in range(config.operation_range, config.num_nodes * config.worker_avg_operations):
        random.choice(worker_nodes).add_operation(random.randint(1, config.operation_range))
        counter += 1
    for node in worker_nodes:
        logging.info(f"Worker {node.node_id} can perform operations {node.operations}")


def sample_work(config: Config, env: simpy.Environment, worker_nodes: List[WorkerNode]):
    while True:
        (completed, wip, complexity) = get_work_counts(worker_nodes)

        sample_times.append(env.now)
        tasks_completed.append(completed)
        work_in_process.append(wip)

        # Queue another sample
        yield env.timeout(config.sample_rate)


def get_work_counts(worker_nodes: List[WorkerNode]) -> (int, int, int):
    completed = 0
    complexity_completed = 0
    wip = 0
    for node in worker_nodes:
        wip += node.wip
        completed += node.tasks_completed
        complexity_completed += node.complexity_completed
    return completed, wip, complexity_completed


def record_task_completion(completion_time, task):
    completion_times.append(completion_time)
    task_ids.append(task.task_id)
    start_times.append(task.start_time)
    processing_times.append(completion_time - task.start_time)
    num_operations.append(task.original_num_operations)


def log(message):
    logging.info(f"{env.now} {message}")


def class_from_args(class_name: object, arg_dict):
    field_set = {f.name for f in fields(class_name) if f.init}
    filtered_arg_dict: dict = {k: v for k, v in arg_dict.items() if k in field_set}
    return class_name(**filtered_arg_dict)


def read_config(config_filename):
    with open(config_filename) as json_file:
        return class_from_args(Config, json.load(json_file))


def store_run_results(run_name: str, worker_nodes: List[WorkerNode]):
    # write data
    output_file = os.path.join(experiment_path, "output", run_name + '-output.csv')
    df = pd.DataFrame(data={'completion_time': completion_times, 'task_id': task_ids, 'start_time': start_times,
                            'process_time': processing_times, 'num_operations': num_operations})
    df.to_csv(output_file, index=False)

    sample_file = os.path.join(experiment_path, "output", run_name + '-samples.csv')
    df2 = pd.DataFrame(data={'sample_time': sample_times, 'tasks_completed': tasks_completed,
                             'work_in_process': work_in_process})
    df2.to_csv(sample_file, index=False)
    run_list.append(run_ctr)
    (completed, wip, complexity) = get_work_counts(worker_nodes)

    final_tasks_completed.append(completed)
    final_work_in_process.append(wip)
    final_complexity_completed.append(complexity)

def store_final_results(config_name: str):
    output_file = os.path.join(experiment_path, "output", "overall-output.csv")
    df = pd.DataFrame(data={'run_num': run_list, 'tasks_completed': final_tasks_completed,
                            'work_in_process': final_work_in_process,
                            'complexity': final_complexity_completed})
    df.to_csv(output_file, index=False)


# Read configuration
parser = argparse.ArgumentParser(description='Run a computational simulation of aggregate intelligence')
parser.add_argument('experiment_dir', help='Experiment directory (containing config file)')
args = parser.parse_args()
experiment_path = args.experiment_dir
experiment_name = os.path.splitext(os.path.basename(experiment_path))[0]
config_name = os.path.join(args.experiment_dir, f"{experiment_name}.json")
if not os.path.exists(config_name):
    print(f'Error, file {config_name} does not exist')
    exit(-1)

# Set up storage
log_dir = os.path.join(args.experiment_dir, "logs")
if not os.path.isdir(log_dir):
    os.mkdir(log_dir)
output_dir = os.path.join(args.experiment_dir, "output")
if not os.path.isdir(output_dir):
    os.mkdir(output_dir)

# Protect against data overwrite
test_log_name = os.path.join(log_dir, 'run1.log')
if os.path.exists(test_log_name):
    print(f"Error, run data already exists for {experiment_name}")
    exit(-1)

# Create SimPy environment and initialize worker nodes
c = read_config(config_name)
if c.seed:
    random.seed(c.seed)
    if c.num_runs != 1:
        print("Forcing num_runs to 0 because seed is set, so multiple runs will produce the same result")
        c.num_runs = 1

# Setup overall run tracking lists
run_list = []
final_tasks_completed = []
final_work_in_process = []
final_complexity_completed = []

# Main experiment loop
is_first_pass = True
for run_ctr in range(1, c.num_runs + 1):
    # Set up run name
    run_name = f"run{run_ctr}"
    log_name = os.path.join(log_dir, run_name + '.log')
    logging.basicConfig(filename=log_name, filemode="w", format='%(message)s', level=logging.INFO, force=True)

    # Clear tracking lists
    completion_times = []
    task_ids = []
    start_times = []
    processing_times = []
    num_operations = []
    sample_times = []
    tasks_completed = []
    work_in_process = []

    # Build the simulation
    env = simpy.Environment()
    nodes = [WorkerNode(c, env, node_id)
             for node_id in range(c.num_nodes)]
    assign_skills(c, nodes)

    # Set up the network
    logging.info(f"Building network type {c.network}")
    if c.network == 'full-connect':
        network = FullyConnectedNetwork()
        entry_nodes = network.setup_network(c, nodes)
    else:
        if c.network == 'random':
            network = RandomNetwork()
            entry_nodes = network.setup_network(c, nodes)
        else:
            print(f"Network type {c.network} is unknown")
            logging.info(f"Network type {c.network} is unknown")
            exit(-1)
    logging.info("Entry nodes are {}".format(sorted([node.node_id for node in entry_nodes])))

    # Start the worker processes
    [env.process(node.process_task()) for node in nodes]

    # Start the task generator process
    env.process(generate_tasks(c, env, entry_nodes))

    # Start the work sampler
    env.process(sample_work(c, env, entry_nodes))

    # Run the simulation for a specific duration
    env.run(until=c.sim_duration)
    store_run_results(run_name, nodes)
    is_first_pass = False
store_final_results(config_name)
