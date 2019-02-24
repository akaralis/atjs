import math
import multiprocessing
import os
import random
import sqlite3
from multiprocessing.pool import Pool

from pandas import Timedelta

from ieee802154.tsch.joining_phase_simulator import JoiningPhaseSimulator, EBSchedulingMethod
from ieee802154.node import Node, NodeType
from ieee802154.node_group import NodeGroup, NodeGroupProperties
from ieee802154.pan_coordinator import PANCoordinator
from ieee802154.tsch import timeslot_template


def main(scheduling_method, atp_enabled=False):
    db_name = "{}{}".format(scheduling_method.name, ("_with_ATP" if atp_enabled else ""))

    db_conn = sqlite3.connect(os.path.join("statistics", "mobile_joining_node", "{}.db".format(db_name)))
    c = db_conn.cursor()

    c.execute('''CREATE TABLE mobile_node_joining_time_samples (advertisers INTEGER, time REAL)''')

    db_conn.commit()

    node_groups_samples_per_test = 1000
    rejoin_attemps = 100
    multislotframe_length = 5  # in slotframes. It is identical to the Enhanced Beacon Interval (EBI)
    slotframe_length = 101
    scanning_duration = (
            2 * multislotframe_length * slotframe_length
            * timeslot_template.defaultTimeslotTemplateFor2450MHzBand.mac_ts_timeslot_length
    )

    num_channels = 16
    eb_length = 50  # in bytes
    randomIns = random.Random()
    channel_switching_time = Timedelta(200, unit="us")
    tx_power = 0  # dBm
    sensitivity = -100  # dBm
    # According to the path loss model that is used (see the function __rx_power in the class JoiningPhaseSimulator),
    # with tx_power = 0 and sensitivity = -100 the guaranteed range is 17m and the max possible distance of a receiver
    # is 60m.

    for num_advertisers in range(10, 71, 10):
        # Note that we assume that all the advertisers are fixed nodes
        node_group_samples = 0

        while node_group_samples < node_groups_samples_per_test:

            # Note that, the ids of nodes affect only (E)CFAS
            if scheduling_method in {EBSchedulingMethod.ECFASH, EBSchedulingMethod.ECFASV}:
                num_available_ids = multislotframe_length * (num_channels - 1) * (2 if atp_enabled else 1)
            else:
                num_available_ids = multislotframe_length * num_channels * (2 if atp_enabled else 1)

            available_ids = randomIns.sample(range(num_available_ids), k=num_available_ids)  # in random order

            ng = NodeGroup(NodeGroupProperties(250000, (100, 100)))
            PANCoordinator(available_ids[0], (ng.properties.area_dimensions[0] * randomIns.random(),
                                              ng.properties.area_dimensions[1] * randomIns.random()), tx_power,
                           sensitivity, Timedelta(0), channel_switching_time, ng)

            # create fixed-nodes/advertisers (except the PAN coordinator)
            for i in range(1, num_advertisers):
                while True:
                    # find a random position that is in the guaranteed range of an already created (fixed) node

                    position = (ng.properties.area_dimensions[0] * randomIns.random(),
                                ng.properties.area_dimensions[1] * randomIns.random())

                    # check if at least one (fixed) node has this position in its range
                    if not all(node.distance_from_point(position) > 17 for node in ng):
                        break

                Node(available_ids[i], position, False, NodeType.FFD, tx_power, sensitivity,
                     Timedelta(randomIns.random() * 100, unit="s"), channel_switching_time, ng)

            # create a mobile node
            initial_pos = (ng.properties.area_dimensions[0] * randomIns.random(),
                           ng.properties.area_dimensions[1] * randomIns.random())

            mobile_node = Node(available_ids[ng.size], initial_pos, True, NodeType.RFD, tx_power, sensitivity,
                               Timedelta(randomIns.random() * 100, unit="s"), channel_switching_time, ng)

            simulator = JoiningPhaseSimulator(
                ng, scheduling_method, timeslot_template.defaultTimeslotTemplateFor2450MHzBand,
                slotframe_length, eb_length, num_channels, scanning_duration, multislotframe_length, atp_enabled)

            simulator.execute()

            # collect samples from the mobile node
            for _ in range(rejoin_attemps):
                res = simulator.rejoining_attempt(mobile_node, Timedelta(randomIns.random() * 100, unit="s"))
                c.execute('''INSERT INTO mobile_node_joining_time_samples(advertisers, time) VALUES(?, ?)''',
                          (num_advertisers, res.total_seconds()))

            db_conn.commit()
            node_group_samples += 1

    db_conn.close()


if __name__ == '__main__':
    PROCESSES_TO_USE = multiprocessing.cpu_count()
    # create a folder for statistics
    os.makedirs(os.path.join("statistics", "mobile_joining_node"), exist_ok=True)

    simulations = [
        (EBSchedulingMethod.ECV,), (EBSchedulingMethod.ECH,),
        (EBSchedulingMethod.Minimal6TiSCH,), (EBSchedulingMethod.ECFASV, False), (EBSchedulingMethod.ECFASV, True),
        (EBSchedulingMethod.CFASV, False), (EBSchedulingMethod.CFASV, True),
        (EBSchedulingMethod.CFASH, False), (EBSchedulingMethod.CFASH, True),
        (EBSchedulingMethod.ECFASH, False), (EBSchedulingMethod.ECFASH, True),

    ]

    with Pool(processes=PROCESSES_TO_USE) as pool:
        pool.starmap(main, simulations)
