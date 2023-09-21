# Code derived from "Network Visualization | NetworkX, Python" by Ning-Yu Kao
# https://medium.com/@kaoningyu/network-visualization-networkx-python-d7b9377c1509

import networkx as nx
import matplotlib.pyplot as plt
import argparse
import os
from dataclasses import dataclass, fields
import json

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


def class_from_args(class_name: object, arg_dict):
    field_set = {f.name for f in fields(class_name) if f.init}
    filtered_arg_dict: dict = {k: v for k, v in arg_dict.items() if k in field_set}
    return class_name(**filtered_arg_dict)


def read_config(config_filename):
    with open(config_filename) as json_file:
        return class_from_args(Config, json.load(json_file))


def read_ints_from_command(command):
    with os.popen(command) as stream:
        nodes = []
        line = stream.readline()
        while line:
            nodes.append(int(line))
            line = stream.readline()
    stream.close()
    return nodes


# Parse arguments and set up the log file
parser = argparse.ArgumentParser(description='Make graphs of all networks for an experiment')
parser.add_argument('experiment_dir', help='Experiment directory (containing config file)')
args = parser.parse_args()
experiment_path = args.experiment_dir
experiment_name = os.path.splitext(os.path.basename(experiment_path))[0]
config_name = os.path.join(args.experiment_dir, f"{experiment_name}.json")
c = read_config(config_name)

log_dir = os.path.join(args.experiment_dir, "logs")

# Set graphics options
# explicitly set positions
pos = {0: (.3, 0), 1: (-.6, 2.5), 2: (1.5, 3.9), 3: (3.6, 2.5), 4: (2.7, 0)}
#pos = {0: (4.53,2.38), 1: (0.00,3.85), 2: (2.80,0.00), 3: (2.80,4.76), 4: (0.00,0.91)}

# network visualization options
options = {
    "font_size": 36,
    "node_size": 3000,
    "node_color": "white",
    "edgecolors": "black",
    "linewidths": 3,
    "width": 2,
}

# Main processing loop
for run_ctr in range(1, c.num_runs + 1):
    # Set up run name
    run_name = f"run{run_ctr}"
    log_name = os.path.join(log_dir, run_name + '.log')
    graph_name = os.path.join(log_dir, run_name + '.png')
    command1 = f"grep connecting {log_name} | sed -n 'p;n' | cut -d' ' -f2"
    command2 = f"grep connecting {log_name} | sed -n 'p;n' | cut -d' ' -f6"
    from_nodes = read_ints_from_command(command1)
    to_nodes = read_ints_from_command(command2)
    G = nx.Graph()
    # add edges
    for ctr in range(len(from_nodes)):
        G.add_edge(from_nodes[ctr], to_nodes[ctr])

    # draw network
    nx.draw_networkx(G, pos, **options)

    # plot setting and show
    ax = plt.gca()
    ax.margins(0.20)
    plt.axis("off")
    plt.savefig(graph_name)
    plt.close()
    G.clear()
