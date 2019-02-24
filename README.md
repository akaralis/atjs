## Installation

* Install Python 3.7
* Clone or download this repository
* Install the Python dependencies:
   `cd atjs` and `pip install -r requirements.txt`
   
## Code Organization

* `ieee802154/`: Includes code related to the IEEE802.15.4 standard.
    * `tsch/`: Includes code related to the TSCH mode of the IEEE802.15.4 standard.
        * `joining_phase_simulator.py`: Simulates the (re)joining attempt of a node in an IEEE802.15.4-TSCH network. 
            Each (re)joining attempt finishes when an EB is received.
        * `timeslot_template`: Used for the definition of the timeslot template.
    * `node.py`: Code for the creation of nodes.
    * `node_group.py`: Code to define a group of nodes that are expected to form a network.
    * `pan_coordinator.py`: Code for the creation of a PAN (Personal Area Network) coordinator. 
* `sim_for_fixed_joining_node.py`: Executes simulations for the case of a fixed joining node.
* `sim_for_mobile_joining_node.py`: Executes simulations for the case of a mobile joining node.
* `results_export.py`: Gets the statistical samples that are produced by the simulations for a fixed and a mobile joining
   node, and exports the average joining time and 95% confidence intervals. The confidence intervals are calculated via
   the bootstrap (statistical) method.

## Usage

To reproduce the statistical results used by the paper, run the following commands:

1. `python3 sim_for_fixed_joining_node.py`
1. `python3 sim_for_mobile_joining_node.py`
1. `python3 results_export.py`

The execution of these commands leads to the creation of the following two folders (in the current working directory): 

- statistics: the samples that were produced by the simulations. The samples are saved in Sqlite databases.
- filtered_statistics: the final statistics (avg joining time and 95% confidence intervals), which are saved as CSV
  files. 

We note that, both the samples and the filtered statistics are provided separately for the examined cases of 
a fixed and a mobile joining node, in the related subfolders. In the case of a fixed joining node, the simulation
results of ECFAS, ECV and ECH are divided into two cases: (a) "one-hop", where the joining  node is 
a neighbor of the PAN coordinator and (b) "two-hops", where the joining node is not a neighbor of the PAN coordinator 
(in this case it is guaranteed that the joining node can not receive frames from the PAN coordinator).