import math
import multiprocessing
import os
import random
import sqlite3
from enum import Enum
from multiprocessing.pool import Pool

from pandas import Timedelta

from ieee802154.tsch.joining_phase_simulator import JoiningPhaseSimulator, EBSchedulingMethod
from ieee802154.node import Node, NodeType
from ieee802154.node_group import NodeGroupProperties, NodeGroup
from ieee802154.pan_coordinator import PANCoordinator
from ieee802154.tsch import timeslot_template


class Scenario(Enum):
    ONE_HOP = "ONE HOP TOPOLOGY"  # for the case where the PAN coordinator is included in the neighbors list
    TWO_HOPS = "TWO HOPs TOPOLOGY"  # for the case where the PAN coordinator is not included in the neighbors list
    ANY = "ANY"  # when the presence of the PAN coordinator in the neighbors list does not affect the performance


def main(scheduling_method, selected_scenario, atp_enabled=False):
    # Create db tables for statistics
    db_name = "{}{}{}".format(scheduling_method.name, ("_with_ATP" if atp_enabled else ""),
                              ("_{}".format(selected_scenario.name) if selected_scenario is not Scenario.ANY else ""))

    if selected_scenario is Scenario.ANY:
        selected_scenario = Scenario.ONE_HOP  # of course we can alternatively use the TWO HOPs scenario

    db_conn = sqlite3.connect(os.path.join("statistics", "fixed_joining_node", "{}.db".format(db_name)))
    c = db_conn.cursor()

    if scheduling_method not in {EBSchedulingMethod.ECV, EBSchedulingMethod.ECH}:
        c.execute('''CREATE TABLE joining_time_samples (neighboring_advertisers INTEGER, time REAL)''')
    else:
        c.execute('''CREATE TABLE joining_time_samples (neighboring_advertisers INTEGER, time REAL, 
        eb_scheduling_delay REAL, num_adv_slots_sensed INTEGER)''')

    c.execute('''CREATE INDEX index2 ON joining_time_samples (neighboring_advertisers)''')
    db_conn.commit()

    boot_time_samples = 1000
    rejoin_attemps = 100
    slotframe_length = 101
    multislotframe_length = 5  # in slotframes. It is identical to the Enhanced Beacon Interval (EBI)
    scanning_duration = (
            2 * multislotframe_length * slotframe_length * timeslot_template.defaultTimeslotTemplateFor2450MHzBand.mac_ts_timeslot_length
    )
    num_channels = 16
    eb_length = 50  # in bytes
    tx_power = 0  # dBm
    sensitivity = -100  # dBm
    # According to the path loss model that is used (see the function __rx_power in the class JoiningPhaseSimulator),
    # with tx_power = 0 and sensitivity = -100 the guaranteed range is 17m and the max possible distance of a receiver
    # is 60m.

    channel_switching_time = Timedelta(200, unit="us")
    randomIns = random.Random()

    def random_position_in_range(antenna_pos, radio_range):
        # Consider the range as a circle
        r = radio_range * math.sqrt(randomIns.random())
        theta = randomIns.random() * 2 * math.pi
        x = antenna_pos[0] + r * math.cos(theta)
        y = antenna_pos[1] + r * math.sin(theta)
        return x, y

    def random_circle_circumference_point(center, r):
        theta = random.random() * 2 * math.pi
        return center[0] + math.cos(theta) * r, center[1] + math.sin(theta) * r

    for num_advertisers in range(1, 11):  # the number of advertisers around the joining node

        for _ in range(boot_time_samples):
            ng = NodeGroup(NodeGroupProperties(250000, (200, 200)))

            # Note that, the ids of nodes affect only (E)CFAS
            if scheduling_method in {EBSchedulingMethod.ECFASH, EBSchedulingMethod.ECFASV}:
                num_available_ids = multislotframe_length * (num_channels - 1) * (2 if atp_enabled else 1)
            else:
                num_available_ids = multislotframe_length * num_channels * (2 if atp_enabled else 1)

            available_ids = randomIns.sample(range(num_available_ids), k=num_available_ids)  # in random order

            # special case where the neighboring advertisers have consecutive ids
            # available_ids = list(range(num_available_ids))

            if selected_scenario is Scenario.ONE_HOP:
                nodes_to_create = num_advertisers + 1
                PANCoordinator(available_ids[0], (100, 100), tx_power, sensitivity, Timedelta(0),
                               channel_switching_time, ng)

                joining_node_pos = random_position_in_range(ng.pan_coordinator.position, 17)
                joining_node = Node(available_ids[1], joining_node_pos, False, NodeType.FFD, tx_power, sensitivity,
                                    Timedelta(randomIns.random() * 100, unit="s"), channel_switching_time, ng)

                for i in range(nodes_to_create - 2):
                    position = random_position_in_range(joining_node.position, 17)
                    Node(available_ids[ng.size], position, False, NodeType.FFD, tx_power, sensitivity,
                         Timedelta(randomIns.random() * 100, unit="s"), channel_switching_time, ng)

            else:
                nodes_to_create = num_advertisers + 2  # including the PAN coordinator

                # in the two hop case we use low tx power (-20dBm) for the PAN coordinator in order to avoid its EBs to
                # reach the joining node -> guaranteed range 5m, max 19m and average 10m
                PANCoordinator(available_ids[0], (100, 100), -20, sensitivity, Timedelta(0), channel_switching_time, ng)
                one_hop_node = Node(available_ids[ng.size],
                                    random_circle_circumference_point(ng.pan_coordinator.position, 10),
                                    False, NodeType.FFD, tx_power, sensitivity,
                                    Timedelta(randomIns.random() * 100, unit="s"), channel_switching_time, ng)

                while True:
                    joining_node_pos = random_position_in_range(one_hop_node.position, 17)
                    if ng.pan_coordinator.distance_from_point(joining_node_pos) > 19:
                        break

                joining_node = Node(available_ids[ng.size], joining_node_pos, False, NodeType.FFD, tx_power,
                                    sensitivity, Timedelta(randomIns.random() * 100, unit="s"), channel_switching_time,
                                    ng)

                for i in range(nodes_to_create - 3):
                    position = random_position_in_range(joining_node.position, 17)
                    Node(available_ids[ng.size], position, False, NodeType.FFD, tx_power, sensitivity,
                         Timedelta(randomIns.random() * 100, unit="s"), channel_switching_time, ng)

            simulator = JoiningPhaseSimulator(
                ng, scheduling_method, timeslot_template.defaultTimeslotTemplateFor2450MHzBand, slotframe_length,
                eb_length, num_channels, scanning_duration, multislotframe_length, atp_enabled)

            simulator.execute()

            for _ in range(rejoin_attemps):
                res = simulator.rejoining_attempt(joining_node, Timedelta(randomIns.random() * 100, unit="s"))
                if scheduling_method not in {EBSchedulingMethod.ECV, EBSchedulingMethod.ECH}:
                    c.execute('''INSERT INTO joining_time_samples(neighboring_advertisers, time)  VALUES (?, ?)''',
                              (num_advertisers, res.total_seconds()))
                else:
                    c.execute('''INSERT INTO joining_time_samples(neighboring_advertisers, time, 
                    eb_scheduling_delay, num_adv_slots_sensed)  VALUES (?, ?, ?, ?)''',
                              (num_advertisers, res[0].total_seconds(), res[1].total_seconds(), res[2]))

            db_conn.commit()

    db_conn.close()


if __name__ == '__main__':
    PROCESSES_TO_USE = multiprocessing.cpu_count()
    # create a folder for statistics
    os.makedirs(os.path.join("statistics", "fixed_joining_node"), exist_ok=True)

    simulations = [
        (EBSchedulingMethod.CFASV, Scenario.ANY, False), (EBSchedulingMethod.CFASV, Scenario.ANY, True),
        (EBSchedulingMethod.CFASH, Scenario.ANY, False), (EBSchedulingMethod.CFASH, Scenario.ANY, True),
        (EBSchedulingMethod.ECFASV, Scenario.ONE_HOP, False), (EBSchedulingMethod.ECFASV, Scenario.ONE_HOP, True),
        (EBSchedulingMethod.ECFASV, Scenario.TWO_HOPS, False), (EBSchedulingMethod.ECFASV, Scenario.TWO_HOPS, True),
        (EBSchedulingMethod.ECFASH, Scenario.ONE_HOP, False), (EBSchedulingMethod.ECFASH, Scenario.ONE_HOP, True),
        (EBSchedulingMethod.ECFASH, Scenario.TWO_HOPS, False), (EBSchedulingMethod.ECFASH, Scenario.TWO_HOPS, True),
        (EBSchedulingMethod.ECV, Scenario.ONE_HOP), (EBSchedulingMethod.ECV, Scenario.TWO_HOPS),
        (EBSchedulingMethod.ECH, Scenario.ONE_HOP), (EBSchedulingMethod.ECH, Scenario.TWO_HOPS),
        (EBSchedulingMethod.Minimal6TiSCH, Scenario.ANY),
        (EBSchedulingMethod.MAC_BASED_AS, Scenario.ANY),
        (EBSchedulingMethod.EMAC_BASED_AS, Scenario.ONE_HOP),
        (EBSchedulingMethod.EMAC_BASED_AS, Scenario.TWO_HOPS)
    ]
    # Note that only ECFAS, ECV, and ECH are favored by the presence of the PAN coordinator in the neighbors list of a
    # joining node

    with Pool(processes=PROCESSES_TO_USE) as pool:
        pool.starmap(main, simulations)
