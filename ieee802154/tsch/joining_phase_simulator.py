import math
import random
import warnings
from bisect import bisect_left
from collections import deque
from enum import Enum

import netaddr
from pandas import Timedelta

from ieee802154.node import NodeType
from ieee802154.node_group import NodeGroup
from ieee802154.tsch.timeslot_template import TimeslotTemplate


class EBSchedulingMethod(Enum):
    CFASV = "Collision-Free Advertisement Scheduling - Vertical Version"
    MAC_BASED_AS = """A modified version of CFAS that calculates the advertisement cell of an advertiser based on 
                its EUI64 address"""  # for test purposes only - not collision-free

    CFASH = "Collision-Free Advertisement Scheduling - Horizontal Version"
    ECFASV = "Enhanced Collision-Free Advertisement Scheduling - Vertical Version"
    EMAC_BASED_AS = """A modified version of ECFAS that calculates the advertisement cell of an advertiser based on 
                its EUI64 address"""  # for test purposes only - not collision-free

    ECFASH = "Enhanced Collision-Free Advertisement Scheduling - Horizontal Version"
    ECV = "Enhanced Coordinated Vertical filling"
    ECH = "Enhanced Coordinated Horizontal filling"
    Minimal6TiSCH = "Minimal 6TiSCH Configuration"


class NotValidJoiningPhaseSimulatorConfig(Exception):
    pass


class JoiningPhaseSimulator:
    def __init__(self, node_group, scheduling_method, timeslot_template, slotframe_length, eb_length, num_channels,
                 scan_duration, ebi, atp_enabled=False):
        """
        :param node_group: the group of nodes on which the simulation will be run. In the current version of the code,
        the configuration of the node group must be done before the use of JoiningPhaseSimulator object and must not be
        changed until the use of the object is completed
        :type node_group: ieee802154.node_group.NodeGroup
        :param scheduling_method: the method to be used for the scheduling of EBs
        :type scheduling_method: EBSchedulingMethod
        :param timeslot_template: the timeslot template that will be used by the network
        :type timeslot_template: ieee802154.tsch.timeslot_template.TimeslotTemplate
        :param slotframe_length: the number of slots in the slotframe
        :type slotframe_length: int
        :param eb_length: the EB length (without the physical layer overhead), in bytes
        :type eb_length: int
        :param num_channels: the number of channels that will be used for the advertisement of the network
        :type num_channels: int
        :param scan_duration: the time that a joining node stays on a channel to find an EB
        :type scan_duration: pandas.Timedelta
        :param ebi: the Enhanced Beacon Interval; the interval between two consecutive EB transmissions of an advertiser,
        expressed in slotframes. It is identical to the multi-slotframe structure length.
        :type ebi: int
        :param atp_enabled: a boolean value indicates whether or not ATP(Advertisement Slot Partitioning) will be used
        :type atp_enabled: bool
        :raise NotValidJoiningPhaseSimulatorConfig: if the provided arguments are not valid
        """

        self.__node_group = node_group
        self.__scheduling_method = scheduling_method
        self.__timeslot_template = timeslot_template
        self.__slotframe_length = slotframe_length
        self.__eb_length = eb_length  # in bytes
        self.__num_channels = num_channels
        self.__scan_duration = scan_duration
        self.__ebi = ebi
        self.__atp_enabled = atp_enabled

        self.__check_arguments()

        # Calculate the transmission time of an EB
        # Include the six bytes of the physical layer overhead
        self.__t_eb = Timedelta((self.__eb_length * 8 + 48) / node_group.properties.data_rate, unit="s")

        # Calculate the number of available advertisement (sub)slots per advertisement slot
        # For convenience, when ATP is not enabled, we consider that each advertisement slot consists of one subslot;
        # that is in this case an advertisement slot is identical with a subslot
        self.__subslot_length = timeslot_template.mac_ts_tx_offset + self.__t_eb
        self.__subslots_per_adv_slot = (
            timeslot_template.mac_ts_timeslot_length // self.__subslot_length if atp_enabled else 1
        )

        # number of slots in the multi-slotframe
        self.__num_slots_in_ms = slotframe_length * self.__ebi

        if scheduling_method in {EBSchedulingMethod.ECV, EBSchedulingMethod.ECH, EBSchedulingMethod.Minimal6TiSCH}:
            # The positions of the advertisement slots in the multi-slotframe
            self.__adv_slots_pos_in_ms = [i for i in range(0, self.__num_slots_in_ms, slotframe_length)]
            # In the case of Minimal6TiSCH only the advertisement cell in the channel offset 0 is used
        else:  # (E)CFAS
            # Our implementation of (E)CFAS determines automatically the number of advertisement slots required to
            # support collision-free EB transmissions
            if scheduling_method in {EBSchedulingMethod.CFASV, EBSchedulingMethod.CFASH,
                                     EBSchedulingMethod.MAC_BASED_AS}:
                num_required_adv_slots = int(
                    math.ceil(
                        self.__node_group.num_ffds / (self.__num_channels * self.__ebi * self.__subslots_per_adv_slot))
                )  # in the slotframe

                if num_required_adv_slots > self.__slotframe_length:
                    raise NotValidJoiningPhaseSimulatorConfig(
                        '''The number of slots is less than required to provide collision-free EB transmissions''')

                total_adv_cells = (
                        num_required_adv_slots * self.__subslots_per_adv_slot * self.__ebi * self.__num_channels
                )

                if scheduling_method is not EBSchedulingMethod.MAC_BASED_AS:
                    temp = set()
                    for node in self.__node_group:
                        if node.type is NodeType.FFD:
                            adv_cell_idx = node.id % total_adv_cells
                            if adv_cell_idx in temp:
                                raise NotValidJoiningPhaseSimulatorConfig(
                                    '''The specified node IDs do not allow for a one-to-one mapping between the nodes and the 
                                    available advertisement cells. Therefore, the EB schedule cannot be collision-free''')
                            else:
                                temp.add(adv_cell_idx)

            else:  # ECFAS - EMAC based AS
                num_required_adv_slots = int(
                    math.ceil((self.__node_group.num_ffds - 1) / ((self.__num_channels - 1) *
                                                                  self.__ebi *
                                                                  self.__subslots_per_adv_slot))
                )  # in the slotframe

                if num_required_adv_slots > self.__slotframe_length:
                    raise NotValidJoiningPhaseSimulatorConfig('''The number of slots is less than required to provide 
                    collision-free EB transmissions''')

                # the number of advertisement cells excluding those allocated to the PAN coordinator
                adv_cells_not_for_pan_c = (
                        num_required_adv_slots * self.__subslots_per_adv_slot * self.__ebi * (self.__num_channels - 1)
                )

                if scheduling_method is not EBSchedulingMethod.EMAC_BASED_AS:
                    temp = set()
                    for node in self.__node_group:
                        if node is not self.__node_group.pan_coordinator and node.type is NodeType.FFD:
                            adv_cell_idx = node.id % adv_cells_not_for_pan_c
                            if adv_cell_idx in temp:
                                raise NotValidJoiningPhaseSimulatorConfig('''The specified node IDs do not allow for a 
                                one-to-one mapping between the nodes and the available advertisement cells. Therefore, the 
                                EB schedule cannot be collision-free''')

                            else:
                                temp.add(adv_cell_idx)

            self.__adv_slots_pos_in_ms = [j for i in range(0, self.__num_slots_in_ms, slotframe_length)
                                          for j in range(i, i + num_required_adv_slots)]

        self.__num_adv_slots_in_ms = len(self.__adv_slots_pos_in_ms)  # advertisement slots in the multi-slotframe

        # The start time of the first slot is equal to the boot_time of the pan coordinator
        self.__slot_0_start_time = self.__node_group.pan_coordinator.boot_time
        self.__node_group._NodeGroup__time = self.__slot_0_start_time
        self.__total_adv_subslots_in_ms = self.__num_adv_slots_in_ms * self.__subslots_per_adv_slot

        self.__has_the_execute_func_been_called = False
        self.__warnings()

        # We precalculate the serial subslot number (ssn), which the serial number of an advertisement subslot within
        # the slotframe containing it. The ssn is defined only when ATP is practically enabled (i.e. we have more than
        # one subslots per advertisement slot).
        # The calculation of ssn may need to be changed if we use non-consecutive advertisements in the future
        if self.__subslots_per_adv_slot > 1:
            self.__ssn = [
                i for i in range(self.__subslots_per_adv_slot)  # define the ssn for the subslots of the first adv slot
            ]  # The index is the incremental number of a subslot within the multi-slotframe structure

            for i in range(1, self.__num_adv_slots_in_ms):
                if self.__adv_slots_pos_in_ms[i - 1] // slotframe_length == (
                        self.__adv_slots_pos_in_ms[i] // slotframe_length):
                    next_ssn = self.__ssn[-1] + 1
                else:
                    next_ssn = 0

                for j in range(self.__subslots_per_adv_slot):
                    self.__ssn.append(next_ssn)
                    next_ssn += 1

        self.__randgen = random.Random()

    def execute(self):
        """
        Simulates the network formation process.
        The simulation is repeated at each call of the function
        :return: a tuple containing the time at which all the nodes have synchronized to the network,
        and the sum energy consumption
        :rtype: (pandas.Timedelta, float)
        """
        # The variable self.__allocated_ch_offset is a structure of nested dictionaries that allows the finding of the
        # channel offset assigned to a FFD node for a specific advertisement (sub)slot
        self.__allocated_ch_offset = {node: dict() for node in self.__node_group if node.type is NodeType.FFD}

        # Make scheduling for the pan coordinator
        self.__make_scheduling_for_the_pan_coordinator()

        # Initially, the only joined node is the pan coordinator
        self.__joined_nodes = {self.__node_group.pan_coordinator}
        self.__advertisers = {self.__node_group.pan_coordinator}  # joined nodes transmitting EBs
        self.__unjoined_nodes = {node for node in self.__node_group if node is not self.__node_group.pan_coordinator}

        self.__sync_asn = dict()  # node->sync_asn - the asn at the time of synchronization
        self.__sync_asn[self.__node_group.pan_coordinator] = 0

        # a counter for the number of EBs that an advertiser has sent
        self.__EB_tx_counter = dict()
        self.__EB_tx_counter[self.__node_group.pan_coordinator] = 0

        self.__formation_asn = None  # the asn when all the nodes have been synchronized to the network

        # Declare when an unjoined node starts to scan for EBs
        self.__scan_start_time = {node: node.boot_time for node in self.__node_group
                                  if node is not self.__node_group.pan_coordinator}

        if self.__scheduling_method in {EBSchedulingMethod.ECV, EBSchedulingMethod.ECH}:
            # The following variable shows which advertisers will sense (after their joining) each advertisement cell
            self.__sensing_nodes = {
                (adv_subslot_idx, ch_offset): set()
                for adv_subslot_idx in range(self.__total_adv_subslots_in_ms)
                for ch_offset in range(1, self.__num_channels)
            }

            # the number of slots that an advertiser senses in order to find a (seemingly) free advertisement cell
            self.__num_slots_sensed = {node: 0 for node in self.__node_group if node.type is NodeType.FFD}

        self.__multislotframe_idx = 0
        self.__has_the_execute_func_been_called = True

        total_time = self.__run_simulation()
        energy_consumption = self.__total_energy_consumption()
        return total_time, energy_consumption

    def rejoining_attempt(self, node, start_time_offset):
        """
        Simulates the rejoining attempt of a node.
        The rejoining attempt runs on the network that was formatted by the last call of the execute function.
        If the execute function has not previously called, then it is automatically called before the rejoining attempt
        :param node: the node of the group that will disconnect from the network and will attempt to rejoin
        :type node: ieee802154.node.Node
        :param start_time_offset: how much time after the current time the rejoining attempt will start
        :type start_time_offset: pandas.Timedelta
        :return:
        If the node is RFD (Reduced Functional Device) then it returns only the joining time (the time elapsed between
        the start and the completion of the rejoining attempt).
        Otherwise:
        For all the methods except ECV and ECV, it returns only the joining time.
        In cases of ECV and ECH, it returns a tuple consisting in order of the following elements: the joining time,
        the time between joining and finding a seemingly free advertisement cell, and the number of advertisement cells
        sensed by the node (after the joining) until it finds a free one
        :rtype: pandas.Timedelta
        """
        if not self.__has_the_execute_func_been_called:
            self.execute()
            self.__has_the_execute_func_been_called = True

        # check if the node belongs to the node group
        if node.node_group is not self.__node_group:
            raise ValueError("The specified node does not belong to the node group")

        # Remove the node from the joined_nodes and add it to unjoined_nodes.
        # If the node is FFD, remove all the advertisement cells it has allocated
        self.__joined_nodes.remove(node)
        self.__advertisers.discard(node)

        if node.type is NodeType.FFD:
            self.__allocated_ch_offset[node].clear()

        self.__unjoined_nodes.add(node)

        multislotframe_length = self.__num_slots_in_ms * self.__timeslot_template.mac_ts_timeslot_length

        start_time = self.__node_group.time + start_time_offset
        self.__multislotframe_idx = start_time // multislotframe_length
        time_offset_in_ms = start_time % multislotframe_length

        # find the position (in the multi-slotrame) of the slot in which the start time falls
        rsn = time_offset_in_ms // self.__timeslot_template.mac_ts_timeslot_length  # relative slot number

        # find the index of the first advertisement slot the node will meet while is listening for EBs
        temp = bisect_left(self.__adv_slots_pos_in_ms, rsn)
        adv_slot_idx = 0 if temp == self.__num_adv_slots_in_ms else temp

        # find the index of the first advertisement subslot the node will meet while is listening for EBs
        if self.__adv_slots_pos_in_ms[adv_slot_idx] == rsn:  # if the start time falls in an advertisement slot
            # find the position of the subslot inside the slot
            subslot_pos = (
                    (time_offset_in_ms % self.__timeslot_template.mac_ts_timeslot_length) // self.__subslot_length
            )
            # find the time elapsed since the start of the subslot
            time_elapsed = (
                    (time_offset_in_ms % self.__timeslot_template.mac_ts_timeslot_length) % self.__subslot_length
            )

            if time_elapsed <= self.__timeslot_template.mac_ts_tx_offset + self.__timeslot_template.mac_ts_rx_wait / 2:
                adv_subslot_idx = adv_slot_idx * self.__subslots_per_adv_slot + subslot_pos
            elif subslot_pos < self.__subslots_per_adv_slot - 1:  # go to the next subslot
                adv_subslot_idx = adv_slot_idx * self.__subslots_per_adv_slot + subslot_pos + 1
            else:  # go to the first subslot of the next advertisement slot
                adv_slot_idx = (adv_slot_idx + 1) % self.__num_adv_slots_in_ms
                adv_subslot_idx = adv_slot_idx * self.__subslots_per_adv_slot
                if adv_slot_idx == 0:  # the next advertisement slot is in the next multislotframe
                    self.__multislotframe_idx += 1
        else:
            if adv_slot_idx == 0:  # the next advertisement slot is in the next multislotframe
                self.__multislotframe_idx += 1
            adv_subslot_idx = adv_slot_idx * self.__subslots_per_adv_slot

        self.__scan_start_time[node] = start_time
        finish_time = self.__run_simulation(adv_subslot_idx)  # the time when the node joined the network

        # If the node is RFD (Reduced Functional Device) then return only the joining time.
        # Otherwise:
        #   For all the methods except ECV and ECV, return only the joining time.
        #   In cases of ECV and ECH, return a tuple consisting of the joining time, the time between joining and
        #   finding a seemingly free advertisement cell, and the number of advertisement cells sensed by the node
        #  (after the joining) until it finds a free one
        joining_time = finish_time - start_time
        if (self.__scheduling_method not in {EBSchedulingMethod.ECV, EBSchedulingMethod.ECH}
                or node.type is NodeType.RFD):
            return joining_time
        else:
            eb_scheduling_delay = self.__node_group.time - finish_time

            # Note: the node starts the sensing at the next multi-slotframe after the joining
            sensing_period_duration = eb_scheduling_delay - multislotframe_length + finish_time % multislotframe_length

            if self.__scheduling_method is EBSchedulingMethod.ECV:
                num_adv_slots_sensed = math.ceil(
                    sensing_period_duration.total_seconds() / multislotframe_length.total_seconds()
                )

            else:
                # convert self.__slotframe_length to Timedelta
                slotframe_length = self.__slotframe_length * self.__timeslot_template.mac_ts_timeslot_length
                num_adv_slots_sensed = math.ceil(
                    sensing_period_duration.total_seconds() / slotframe_length.total_seconds()
                )

            return joining_time, eb_scheduling_delay, num_adv_slots_sensed

    def __run_simulation(self, starting_adv_subslot=0):
        starting_i = starting_adv_subslot // self.__subslots_per_adv_slot  # starting advertisement slot
        starting_j = starting_adv_subslot % self.__subslots_per_adv_slot  # starting subslot in the advertisement slot
        network_formation_time = None

        while True:
            for i in range(starting_i, self.__num_adv_slots_in_ms):
                # Calculate the asn of the current advertisement slot.
                # The asn of a subslot is the asn of the advertisement slot to which belongs.
                asn = self.__multislotframe_idx * self.__num_slots_in_ms + self.__adv_slots_pos_in_ms[i]

                for j in range(starting_j, self.__subslots_per_adv_slot):
                    adv_subslot_idx = i * self.__subslots_per_adv_slot + j
                    ssn = self.__ssn[adv_subslot_idx] if self.__subslots_per_adv_slot > 1 else None

                    # update the EB_tx_counter of advertisers
                    for advertiser in self.__advertisers:
                        # Check if the advertiser transmits in the current advertisement (sub)slot
                        if adv_subslot_idx in self.__allocated_ch_offset[advertiser]:
                            self.__EB_tx_counter[advertiser] += 1

                    # execute sensing
                    if self.__scheduling_method in {EBSchedulingMethod.ECV, EBSchedulingMethod.ECH}:
                        sensing_nodes_new = dict()

                        for ch_offset in range(1, self.__num_channels):
                            nodes_sense_ch = self.__sensing_nodes[(adv_subslot_idx, ch_offset)]
                            nodes_sense_ch_busy = set()

                            for node in nodes_sense_ch:
                                self.__num_slots_sensed[node] += 1

                                if self.__is_a_neighbor_transmitting(node, adv_subslot_idx, ch_offset):
                                    nodes_sense_ch_busy.add(node)

                            nodes_sense_ch_free = nodes_sense_ch - nodes_sense_ch_busy

                            for node in nodes_sense_ch_free:
                                self.__allocated_ch_offset[node][adv_subslot_idx] = ch_offset

                            self.__sensing_nodes[adv_subslot_idx, ch_offset] = set()  # clean

                            if not (adv_subslot_idx == self.__total_adv_subslots_in_ms - 1
                                    and ch_offset == self.__num_channels - 1):
                                if self.__scheduling_method is EBSchedulingMethod.ECV:
                                    if ch_offset == self.__num_channels - 1:
                                        sensing_nodes_new[adv_subslot_idx + 1, 1] = nodes_sense_ch_busy
                                    else:
                                        sensing_nodes_new[adv_subslot_idx, ch_offset + 1] = nodes_sense_ch_busy
                                else:  # EBSchedulingMethod.ECH
                                    if adv_subslot_idx == self.__total_adv_subslots_in_ms - 1:
                                        sensing_nodes_new[0, ch_offset + 1] = nodes_sense_ch_busy
                                    else:
                                        sensing_nodes_new[adv_subslot_idx + 1, ch_offset] = nodes_sense_ch_busy
                            else:
                                # ECV and ECH do not describe what happens if a free advertisement cell is not found
                                # We assign a random advertisement cell in this case
                                for node in nodes_sense_ch_busy:
                                    self.__allocated_ch_offset[node][
                                        random.randint(0, self.__total_adv_subslots_in_ms - 1)
                                    ] = random.randint(1, self.__num_channels - 1)

                        for key, value in sensing_nodes_new.items():
                            self.__sensing_nodes[key] = value

                    new_joined_nodes = set()
                    new_advertisers = set()
                    tx_start_time = {}  # gives the EB transmission start time of a specified advertiser in this subslot
                    for node in self.__unjoined_nodes:
                        current_adv_subslot_start_time = (self.__slot_0_start_time
                                                          + asn * self.__timeslot_template.mac_ts_timeslot_length
                                                          + j * self.__subslot_length)

                        # Update the node group time to show the perfect start transmission time.
                        # Because of the clock drift the transmission of a node may be start before or after this time.
                        # The update of the node group time is necessary for the mobility simulation of the nodes.
                        # It is worth noting that the movement of the nodes during an EB transmission or more general
                        # within a (sub)slot is negligible.
                        self.__node_group._NodeGroup__time = (
                                current_adv_subslot_start_time + self.__timeslot_template.mac_ts_tx_offset
                        )

                        candidate_ebs = []  # EBs that can reach the node

                        for advertiser in self.__advertisers:
                            # Check if the advertiser transmits in the current advertisement (sub)slot
                            if adv_subslot_idx not in self.__allocated_ch_offset[advertiser]:
                                continue

                            rx_signal_power = self.__rx_power(
                                advertiser.tx_power,
                                advertiser.distance_from_node(node),
                            )

                            # Check if the transmitted signal can be perceived by the node
                            if rx_signal_power < node.radio_sensitivity:
                                continue

                            if tx_start_time.get(advertiser) is None:  # the tx start time has not been computed

                                # The Maximum Allowed Clock Drift of a synchronized (joined) node
                                macd = self.__timeslot_template.mac_ts_rx_wait / 2  # based on the standard

                                tx_start_time[advertiser] = (
                                        self.__node_group.time +
                                        self.__randgen.random() * self.__randgen.choice([-1, 1]) * macd
                                )

                            # We consider the minimum possible propagation delay. In fact, in a Wireless Sensor Network
                            # the nodes are quite close and the propagation delay is negligible. We could ignore it.
                            prop_delay = Timedelta(int(advertiser.distance_from_node(node) * 10 / 3), unit="ns")
                            rx_start_time = tx_start_time[advertiser] + prop_delay
                            tx_channel_offset = self.__allocated_ch_offset[advertiser][adv_subslot_idx]
                            candidate_ebs.append({
                                "rx_start_time": rx_start_time,
                                "rx_power": rx_signal_power,
                                "tx_channel_offset": tx_channel_offset
                            })

                        if len(candidate_ebs) == 0 or self.__captured_eb(node, candidate_ebs, asn, ssn) is None:
                            continue

                        new_joined_nodes.add(node)
                        if self.__sync_asn.get(node) is None:
                            self.__sync_asn[node] = asn

                        if node.type is NodeType.FFD:
                            new_advertisers.add(node)
                            self.__EB_tx_counter[node] = 0
                            if self.__scheduling_method is EBSchedulingMethod.CFASV:
                                self.__cfasv_allocate_adv_cell(node)
                            elif self.__scheduling_method is EBSchedulingMethod.MAC_BASED_AS:
                                self.__mbas_allocate_adv_cell(node)
                            elif self.__scheduling_method is EBSchedulingMethod.CFASH:
                                self.__cfash_allocate_adv_cell(node)
                            elif self.__scheduling_method is EBSchedulingMethod.ECFASV:
                                self.__cfasv_allocate_adv_cell(node, True)
                            elif self.__scheduling_method is EBSchedulingMethod.EMAC_BASED_AS:
                                self.__mbas_allocate_adv_cell(node, True)
                            elif self.__scheduling_method is EBSchedulingMethod.ECFASH:
                                self.__cfash_allocate_adv_cell(node, True)
                            elif self.__scheduling_method is EBSchedulingMethod.Minimal6TiSCH:
                                # Typically, an advertiser starts transmitting EBs after completing the association
                                # process, which can be done in any of the slotframes after the EB reception.
                                # For this reason, herein, we select randomly the slotframe where a new advertiser
                                # starts transmitting EBs
                                self.__allocated_ch_offset[node][
                                    self.__randgen.randint(0, self.__num_adv_slots_in_ms - 1)] = 0
                            else:  # EBSchedulingMethod.ECV or EBSchedulingMethod.ECH
                                self.__sensing_nodes[(0, 1)].add(node)

                    self.__joined_nodes.update(new_joined_nodes)
                    self.__advertisers.update(new_advertisers)
                    self.__unjoined_nodes.difference_update(new_joined_nodes)

                    if len(self.__unjoined_nodes) == 0:
                        if network_formation_time is None:
                            # network_formation_time: the time when the last node joins the network
                            network_formation_time = (self.__slot_0_start_time +
                                                      asn * self.__timeslot_template.mac_ts_timeslot_length +
                                                      (j + 1) * self.__subslot_length)

                        # In the cases of ECV and ECH, the simulation must be continued until all the new advertisers
                        # finish the search for a seemingly free advertisement cell
                        if (
                                self.__scheduling_method not in {EBSchedulingMethod.ECV, EBSchedulingMethod.ECH}
                                or all(len(sensing_nodes) == 0 for sensing_nodes in self.__sensing_nodes.values())
                        ):
                            # update the node group time
                            self.__node_group._NodeGroup__time = (self.__slot_0_start_time +
                                                                  asn * self.__timeslot_template.mac_ts_timeslot_length
                                                                  + (j + 1) * self.__subslot_length)

                            if self.__formation_asn is None:
                                self.__formation_asn = asn

                            # return the network formation time
                            return network_formation_time

                starting_j = 0

            starting_i = 0
            self.__multislotframe_idx += 1

    def __total_energy_consumption(self):
        """
        Returns the sum energy consumption of nodes until all the nodes have been synchronized to the network.
        If no simulation was performed previously, it returns None.
        :return: the average energy consumption if a simulation has run, otherwise None
        :rtype: float
        """

        if not self.__has_the_execute_func_been_called:
            return None

        # We use the energy consumption information of Zolertia RE-Mote
        # https://github.com/Zolertia/Resources/blob/master/RE-Mote/Hardware/Revision%20B/Datasheets/ZOL-RM0x-B%20-%20RE-Mote%20revision%20B%20Datasheet%20v.1.0.0.pdf
        rx_A = 0.02
        tx_A = 0.024
        idle_A = 1.3 / 10 ** 6
        volts = 3.7

        sum_ec = 0
        for node in self.__node_group:

            if self.__scheduling_method in {EBSchedulingMethod.ECV, EBSchedulingMethod.ECH, EBSchedulingMethod.ECFASV,
                                            EBSchedulingMethod.ECFASH,
                                            EBSchedulingMethod.EMAC_BASED_AS} and node is self.__node_group.pan_coordinator:
                continue  # in this case we assume that the node has no energy limitations

            sync_time = self.__sync_asn[node] * self.__timeslot_template.mac_ts_timeslot_length.total_seconds()
            ec_for_sync = sync_time * rx_A * volts  # joules

            EB_tx_counter = self.__EB_tx_counter.get(node, 0)  # return 0 if the node is not an advertiser
            ec_for_EBs = EB_tx_counter * self.__t_eb.total_seconds() * tx_A * volts  # joules

            idle_slots = self.__formation_asn - self.__sync_asn[node] - EB_tx_counter
            ec_idle = idle_slots * self.__timeslot_template.mac_ts_timeslot_length.total_seconds() * idle_A * volts

            sum_ec += ec_for_sync + ec_for_EBs + ec_idle

            if self.__scheduling_method in {EBSchedulingMethod.ECV, EBSchedulingMethod.ECH}:
                num_slots_sensed = self.__num_slots_sensed.get(node, 0)
                sensing_time_per_slot = self.__timeslot_template.mac_ts_rx_wait.total_seconds()
                ec_sensing = num_slots_sensed * sensing_time_per_slot * rx_A * volts
                sum_ec += ec_sensing

        return sum_ec

    def __cfasv_allocate_adv_cell(self, node, enhanced_version=False):
        """
        This function is used to create the EB schedule of a node except the PAN coordinator according to (E)CFASV.
        For the EB schedule of the PAN coordinator, the function __make_scheduling_for_the_pan_coordinator is provided
        """
        # channel offsets available for the EBs of nodes except the PAN coordinator
        num_avail_ch_offsets = self.__num_channels if not enhanced_version else self.__num_channels - 1

        adv_cell_idx = node.id % (self.__total_adv_subslots_in_ms * num_avail_ch_offsets)
        adv_subslot_idx = adv_cell_idx // num_avail_ch_offsets
        ch_offset = adv_cell_idx % num_avail_ch_offsets
        if enhanced_version:
            ch_offset += 1

        self.__allocated_ch_offset[node][adv_subslot_idx] = ch_offset

    def __mbas_allocate_adv_cell(self, node, enhanced_version=False):
        num_avail_ch_offsets = self.__num_channels if not enhanced_version else self.__num_channels - 1

        def sax(mac_addr):  # https://bitbucket.org/6tisch/simulator/src/master/SimEngine/Mote/sf.py
            LEFT_SHIFT_NUM = 5
            RIGHT_SHIFT_NUM = 2

            # assuming v (seed) is 0
            hash_value = 0
            for word in netaddr.EUI(mac_addr).words:
                for byte in divmod(word, 0x100):
                    left_shifted = (hash_value << LEFT_SHIFT_NUM)
                    right_shifted = (hash_value >> RIGHT_SHIFT_NUM)
                    hash_value ^= left_shifted + right_shifted + byte

            # assuming T (table size) is 16-bit
            return hash_value & 0xFFFF

        sax_int = sax(node.mac_address)

        adv_cell_idx = sax_int % (num_avail_ch_offsets * self.__total_adv_subslots_in_ms)
        adv_subslot_idx = adv_cell_idx // num_avail_ch_offsets
        ch_offset = adv_cell_idx % num_avail_ch_offsets
        if enhanced_version:
            ch_offset += 1

        self.__allocated_ch_offset[node][adv_subslot_idx] = ch_offset

    def __cfash_allocate_adv_cell(self, node, enhanced_version=False):
        """
        This function is used to create the EB schedule of a node except the PAN coordinator according to (E)CFASH.
        For the EB schedule of the PAN coordinator, the function __make_scheduling_for_the_pan_coordinator is provided
        """
        # channel offsets available for the EBs of nodes except the PAN coordinator
        num_avail_ch_offsets = self.__num_channels if not enhanced_version else self.__num_channels - 1

        adv_cell_idx = node.id % (self.__total_adv_subslots_in_ms * num_avail_ch_offsets)
        adv_subslot_idx = adv_cell_idx % self.__total_adv_subslots_in_ms
        ch_offset = adv_cell_idx // self.__total_adv_subslots_in_ms
        if enhanced_version:
            ch_offset += 1

        self.__allocated_ch_offset[node][adv_subslot_idx] = ch_offset

    def __make_scheduling_for_the_pan_coordinator(self):
        if self.__scheduling_method is EBSchedulingMethod.Minimal6TiSCH:
            self.__allocated_ch_offset[self.__node_group.pan_coordinator][0] = 0

        elif self.__scheduling_method is EBSchedulingMethod.CFASV:
            self.__cfasv_allocate_adv_cell(self.__node_group.pan_coordinator)

        elif self.__scheduling_method is EBSchedulingMethod.MAC_BASED_AS:
            self.__mbas_allocate_adv_cell(self.__node_group.pan_coordinator)

        elif self.__scheduling_method is EBSchedulingMethod.CFASH:
            self.__cfash_allocate_adv_cell(self.__node_group.pan_coordinator)

        else:  # (E)CFAS, ECV, ECH, ΕMAC-based AS
            # In this case, it is assumed that the coordinator has no energy limitations and transmits EBs
            # in every advertisement (sub)slot using channel offset 0.
            for adv_subslot_idx in range(self.__total_adv_subslots_in_ms):
                self.__allocated_ch_offset[self.__node_group.pan_coordinator][adv_subslot_idx] = 0

    def __channel_calculation(self, ch_offset, asn, ssn=None):
        if ssn is not None:
            return (asn + ssn + ch_offset) % self.__num_channels

        return (asn + ch_offset) % self.__num_channels

    def __is_a_neighbor_transmitting(self, observer, adv_subslot_idx, target_ch_offset):
        for advertiser in self.__advertisers:
            if advertiser is observer:
                continue

            if (
                    self.__allocated_ch_offset[advertiser].get(adv_subslot_idx, -1) == target_ch_offset and
                    self.__rx_power(
                        advertiser.tx_power, advertiser.distance_from_node(observer)
                    ) >= observer.radio_sensitivity
            ):
                return True

        return False

    def __rx_power(self, tx_power, distance):
        # Path loss is calculated according to site-general model of ITU-R P.1238-9 recommendation
        f = 2400  # frequency in Mhz
        Ld0 = 20 * math.log10(f) - 28  # path loss at 1m (reference distance) with Line-Of-Sight (LOS)
        N = 40  # distance power loss coefficient
        Lf = 0  # floor penetration loss factor - We consider that the nodes are on the same floor
        PL = Ld0 + N * math.log10(distance) + Lf  # average path loss
        while True:
            variance = self.__randgen.normalvariate(0, 4)  # shadowing
            if 11 >= variance >= -11:  # extreme values (negligible probability to occur) are rejected
                break

        return tx_power - PL + variance

    def __captured_eb(self, joining_node, candidate_ebs, asn, ssn=None):
        """
        This function checks if the joining node can receive an EB, and if it is possible then the captured EB is
        returned, otherwise the function returns None
        """

        # Synchronization header duration
        SHR_DURATION = Timedelta(5 * 8 / self.__node_group.properties.data_rate, unit="s")
        CAPTURE_EFFECT_THRESHOLD = 3  # dB according to the literature
        captured_eb = None
        interfering_ebs = {c: deque() for c in range(self.__num_channels)}  # per channel offset
        interference = {c: 0 for c in range(self.__num_channels)}  # per channel offset, in mW
        frame_sync_end_time = None

        # internal support functions #
        def dbm_to_mw(dbm_value):
            return 10 ** (dbm_value / 10)

        def mw_to_dbm(mw_value):
            return 10 * math.log10(mw_value)

        def add_interfering_eb(eb):
            interfering_ebs[eb["tx_channel_offset"]].append(eb)
            interference[eb["tx_channel_offset"]] += dbm_to_mw(eb["rx_power"])

        def update_interfering_ebs(ch_offset, update_time):
            # update the interfering list in the given tx offset so that it contains only the EBs that are still
            # transmitted at the update_time
            interfering_ebs[ch_offset] = deque(
                sorted(interfering_ebs[ch_offset], key=lambda elem: elem["rx_start_time"])
            )
            while len(interfering_ebs[ch_offset]) > 0:
                # Note that, the interfering EB list is ordered based on the rx time
                interfering_eb = interfering_ebs[ch_offset][0]
                if interfering_eb["rx_start_time"] + self.__t_eb >= update_time:
                    break

                interference[ch_offset] -= dbm_to_mw(interfering_eb["rx_power"])
                interfering_ebs[ch_offset].popleft()

            if len(interfering_ebs[ch_offset]) == 0:  # fix floating point errors
                interference[ch_offset] = 0

        ###############################

        # sorting in ascending order based on the reception start time
        candidate_ebs.sort(key=lambda elem: elem["rx_start_time"])

        # The drift of node clocks is mainly caused by: (a) Initial Accuracy, (b) Temperature Stability
        # and (c) Aging. Herein, we use a realistic maximum deviation of ±30ppm
        node_clock_drift = (
                self.__randgen.random() * self.__randgen.choice([-1, 1]) * 30 / 10 ** 6
        )  # expressed as a percentage

        for candidate_eb in candidate_ebs:

            if captured_eb is not None and captured_eb["rx_start_time"] + self.__t_eb < candidate_eb["rx_start_time"]:
                return captured_eb  # ok, an EB has already been successfully received

            # check if the node is active when the EB arrives
            if joining_node.boot_time > candidate_eb["rx_start_time"]:
                add_interfering_eb(candidate_eb)  # may collide with a later EB
                continue

            # Calculate the time that the (clock of the) joining node has when the EB arrives
            eb_local_arrival_time = (
                    candidate_eb["rx_start_time"] + candidate_eb["rx_start_time"] * node_clock_drift
            )

            # Check if the remaining time in the current scanning period is enough to receive the EB
            if self.__scan_duration > (
                    eb_local_arrival_time - self.__scan_start_time[joining_node]) % (
                    self.__scan_duration + joining_node.channel_switching_time) + self.__t_eb:
                # Calculate the absolute (serial) number of the channel to which the node listens at the
                # eb_local_arrival_time. The acn (absolute channel number) is equal to the number of
                # channels the node has changed.
                acn = (eb_local_arrival_time - self.__scan_start_time[joining_node]) // (
                        self.__scan_duration + joining_node.channel_switching_time)
            else:
                add_interfering_eb(candidate_eb)  # may collide with a later EB
                continue

            # Find the channel to which the joining node listens when the EB arrives
            # According to the standard, the joining node changes the channels serially
            listening_channel = acn % self.__num_channels

            if listening_channel == self.__channel_calculation(candidate_eb["tx_channel_offset"], asn, ssn):
                update_interfering_ebs(candidate_eb["tx_channel_offset"], candidate_eb["rx_start_time"])

                if captured_eb is None:
                    if interference[candidate_eb["tx_channel_offset"]] == 0:  # new frame synchronization attempt
                        captured_eb = candidate_eb
                        frame_sync_end_time = captured_eb["rx_start_time"] + SHR_DURATION

                    # Check if the new EB cannot be captured
                    # Note that, the frame_sync_end time will be Νone if the transmission of the interfering EBs
                    # started before the joining node started to listen to the channel to which they are transmitted
                    elif (
                            frame_sync_end_time is not None and frame_sync_end_time < candidate_eb["rx_start_time"] or
                            candidate_eb["rx_power"] - mw_to_dbm(interference[candidate_eb["tx_channel_offset"]])
                            < CAPTURE_EFFECT_THRESHOLD
                    ):
                        add_interfering_eb(candidate_eb)
                    else:  # the new EB can be captured
                        captured_eb = candidate_eb
                        if frame_sync_end_time is None:
                            frame_sync_end_time = captured_eb["rx_start_time"] + SHR_DURATION

                # If the captured EB is not None, the candidate EB is transmitted to the channel where the captured EB
                # is transmitted. We have check above that the captured EB has not finished and also that the joining
                # node has enough time to receive it, before it changes channel
                elif (captured_eb["rx_power"] - mw_to_dbm(
                        interference[captured_eb["tx_channel_offset"]] + dbm_to_mw(candidate_eb["rx_power"]))
                      < CAPTURE_EFFECT_THRESHOLD):

                    add_interfering_eb(captured_eb)
                    captured_eb = None

                    if (
                            frame_sync_end_time < candidate_eb["rx_start_time"]
                            or candidate_eb["rx_power"] - mw_to_dbm(interference[candidate_eb["tx_channel_offset"]])
                            < CAPTURE_EFFECT_THRESHOLD
                    ):
                        add_interfering_eb(candidate_eb)
                    else:
                        captured_eb = candidate_eb
            else:
                add_interfering_eb(candidate_eb)

        return captured_eb

    def __check_arguments(self):
        if not isinstance(self.__node_group, NodeGroup):
            raise NotValidJoiningPhaseSimulatorConfig(
                "The parameter node_group should be an instance of {}.{}".format(NodeGroup.__module__,
                                                                                 NodeGroup.__name__))
        if self.__node_group.size == 0:
            raise NotValidJoiningPhaseSimulatorConfig("The node group is empty")
        if self.__node_group.pan_coordinator is None:
            raise NotValidJoiningPhaseSimulatorConfig("The node group does not have a pan coordinator")

        if not isinstance(self.__scheduling_method, EBSchedulingMethod):
            raise NotValidJoiningPhaseSimulatorConfig(
                "The parameter scheduling_method must be an instance of {}.{}".format(EBSchedulingMethod.__module__,
                                                                                      EBSchedulingMethod.__name__))
        if not isinstance(self.__timeslot_template, TimeslotTemplate):
            raise NotValidJoiningPhaseSimulatorConfig(
                "The parameter timeslot_template must be an instance of {}.{}".format(TimeslotTemplate.__module__,
                                                                                      TimeslotTemplate.__name__))
        if not isinstance(self.__slotframe_length, int) or self.__slotframe_length <= 0:
            raise NotValidJoiningPhaseSimulatorConfig(
                "The parameter slotframe_length must be a positive integer")

        if not isinstance(self.__eb_length, int) or self.__eb_length <= 0 or self.__eb_length > 127:
            raise NotValidJoiningPhaseSimulatorConfig(
                "The parameter eb_length must be an integer greater than 0 and less than 128")

        if not isinstance(self.__num_channels, int) or self.__num_channels <= 0:
            raise NotValidJoiningPhaseSimulatorConfig(
                "The parameter num_channels must be a non-negative integer")

        if self.__scheduling_method in {EBSchedulingMethod.ECV, EBSchedulingMethod.ECH, EBSchedulingMethod.ECFASV,
                                        EBSchedulingMethod.ECFASH, EBSchedulingMethod.EMAC_BASED_AS}:
            if (
                    any(node is not self.__node_group.pan_coordinator and node.type is NodeType.FFD
                        for node in self.__node_group) and self.__num_channels == 1
            ):
                raise NotValidJoiningPhaseSimulatorConfig(
                    "{} requires more than one channels".format(self.__scheduling_method.name))

        if not isinstance(self.__scan_duration, Timedelta) or self.__scan_duration <= Timedelta(0):
            raise NotValidJoiningPhaseSimulatorConfig(
                "The parameter scan_period must be a positive timedelta ({}.{})".format(
                    Timedelta.__module__, Timedelta.__name__))

        if not isinstance(self.__ebi, int) or self.__ebi <= 0:
            raise NotValidJoiningPhaseSimulatorConfig("The parameter ebi must be a positive integer")

        if not isinstance(self.__atp_enabled, bool):
            raise NotValidJoiningPhaseSimulatorConfig(
                "The parameter atp_enabled must be of type bool")

        if self.__scheduling_method is EBSchedulingMethod.Minimal6TiSCH:
            if self.__atp_enabled:
                raise NotValidJoiningPhaseSimulatorConfig(
                    "ATP is not supported by the the Minimal 6TiSCH configuration yet")

    def __warnings(self):
        if math.gcd(self.__num_slots_in_ms, self.__num_channels) != 1:
            warnings.warn('''The length of the {} and the number of channels are not relatively prime. It
                          is stressed that the length of the multi-slotframe and the number of channels should be 
                          relatively prime in order to assure that each link rotates through the available channels
                          '''.format("multi-slotframe" if self.__scheduling_method is not
                                                          EBSchedulingMethod.Minimal6TiSCH else "slotframe")
                          )
