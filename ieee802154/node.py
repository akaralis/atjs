import random

import ieee802154
import math
from enum import Enum
from pandas import Timedelta


class NodeType(Enum):
    FFD = "Full Function Device"
    RFD = "Reduced Function Device"


class NotValidNodeConfigError(Exception):
    pass


class Node:
    """
    This class represents nodes and is a friend class of the class ieee802154.node_group.NodeGroup.
    When a node is mobile, the node moves according to the Random Waypoint Model, with a speed range of 0.1 - 5 m/s and
    zero pause times at the waypoints. Each node automatically receives a unique mac address within the node group.
    """

    def __init__(self, id, position, is_mobile, type, tx_power, radio_sensitivity, boot_time, channel_switching_time,
                 node_group):
        """
        :param id: the identifier of the node
        :type id: int
        :param position: the (initial) position of the node expressed in cartesian dimensions
        :type position: (int | float, int | float)
        :param is_mobile: determines if the node is mobile or fixed
        :type is_mobile: bool
        :param type: the type of the node
        :type type: NodeType
        :param tx_power the transmission power of the node, in dBm
        :type int
        :param radio_sensitivity the radio sensitivity of the node, in dBm
        :type int
        :param boot_time: the time it takes for the node to be ready to operate after the power has been turned on
        :type boot_time: pandas.Timedelta
        :param channel_switching_time: the time it takes the node to change channel
        :type channel_switching_time: pandas.Timedelta
        :param node_group: the group to which the node belongs
        :type node_group: ieee802154.node_group.NodeGroup
        :raise NotValidNodeConfigError: : if at least one of the specified parameters is not valid
        """

        # Check if the parameters are valid
        if not isinstance(id, int) or id < 0:
            raise NotValidNodeConfigError("The id of the node must be a non-negative integer")

        elif not isinstance(position, tuple) or not isinstance(position[0], (float, int)) or (
                not isinstance(position[1], (float, int))):
            raise NotValidNodeConfigError("The position of the node must be expressed in cartesian dimensions (x,y)")

        elif (position[0] < 0 or position[1] < 0 or position[0] > node_group.properties.area_dimensions[0]
              or position[1] > node_group.properties.area_dimensions[1]):
            raise NotValidNodeConfigError("The position is not in the specified area")

        elif not isinstance(is_mobile, bool):
            raise NotValidNodeConfigError("The parameter is_mobile should have a boolean value")

        elif not isinstance(type, NodeType):
            raise NotValidNodeConfigError(
                "The type of the node must be an instance of {}.{}".format(NodeType.__module__, NodeType.__name__))

        elif not isinstance(tx_power, int):
            raise NotValidNodeConfigError(
                "The transmission power must be an integer"
            )

        elif not isinstance(radio_sensitivity, int):
            raise NotValidNodeConfigError(
                "The radio sensitivity must be an integer"
            )

        elif not isinstance(boot_time, Timedelta) or boot_time < Timedelta(0):
            raise NotValidNodeConfigError(
                "The boot_time must be a non-negative timedelta ({}.{})".format(Timedelta.__module__,
                                                                                Timedelta.__name__))

        elif not isinstance(channel_switching_time, Timedelta) or channel_switching_time < Timedelta(0):
            raise NotValidNodeConfigError(
                "The channel switching time must be a non-negative timedelta ({}.{})".format(
                    Timedelta.__module__, Timedelta.__name__))

        elif not isinstance(node_group, ieee802154.node_group.NodeGroup):
            raise NotValidNodeConfigError(
                "The node_group must be an instance of {}.{}".format(ieee802154.node_group.NodeGroup.__module__,
                                                                     ieee802154.node_group.NodeGroup.__name__))
        self.__id = id
        self.__initial_position = position
        self.__is_mobile = is_mobile
        self.__type = type
        self.__tx_power = tx_power
        self.__radio_sensitivity = radio_sensitivity
        self.__boot_time = boot_time
        self.__channel_switching_time = channel_switching_time
        self.__node_group = node_group

        if is_mobile:
            self.__randgen = random.Random()
            self.__move = {}
            self.__new_move()

        # Check if a node with the given id already exists in the group
        for node in node_group:
            if self.__id == node.id:
                raise NotValidNodeConfigError("There is already a node with the given id in the group")

        # Add the node to the group
        node_group._NodeGroup__add_node(self)

        # internal support function
        self.__distance = lambda point1, point2: math.sqrt((point1[0] - point2[0]) ** 2 + (point1[1] - point2[1]) ** 2)

    @property
    def id(self):
        """
        :return: the identifier of the node
        :rtype: int
        """
        return self.__id

    @property
    def position(self):
        """
        :return: the position of the node at the current time, expressed in cartesian dimensions
        :rtype: (int | float, int | float)
        """
        if not self.__is_mobile or self.__boot_time > self.__node_group.time:
            return self.__initial_position
        else:
            while True:
                # check if the last move has been completed

                total_distance = self.__distance(self.__move["start_pos"], self.__move["end_pos"])
                t_dif = self.__node_group.time - self.__move["start_t"]
                d = self.__move["speed"] * t_dif.total_seconds()  # d: current distance from the starting point

                if d > total_distance:
                    # The last specified move has already completed. We simulate the movement of the node from
                    # the end of this move to the current node group time, by defining the end time of the last move
                    # as the start time of the new move.
                    self.__new_move(self.__move["start_t"] + Timedelta(total_distance / self.__move["speed"], unit="s"))
                else:
                    break

            x0 = self.__move["start_pos"][0]
            y0 = self.__move["start_pos"][1]
            x1 = self.__move["end_pos"][0]
            y1 = self.__move["end_pos"][1]

            if x0 == x1:
                x = x0
                if y0 < y1:
                    y = y0 + d
                else:
                    y = y0 - d
            else:
                m = (y1 - y0) / (x1 - x0)
                if x0 < x1:
                    x = x0 + d / math.sqrt(1 + m ** 2)
                else:
                    x = x0 - d / math.sqrt(1 + m ** 2)

                y = m * (x - x0) + y0

            return x, y

    @property
    def is_mobile(self):
        """
        :return: True if the node is mobile, otherwise False
        :rtype: bool
        """
        return self.__is_mobile

    @property
    def type(self):
        """
        :return: the type of the node
        :rtype: NodeType
        """
        return self.__type

    @property
    def tx_power(self):
        """
        :return: the transmission power of the node, in dBm
        :rtype int
        """
        return self.__tx_power

    @property
    def radio_sensitivity(self):
        """
        :return: the radio sensitivity of the node, in dBm
        :rtype: int
        """
        return self.__radio_sensitivity

    @property
    def boot_time(self):
        """
        :return: the time it takes for the node to be ready to operate after the power has been turned on
        :rtype: pandas.Timedelta
        """
        return self.__boot_time

    @property
    def channel_switching_time(self):
        """
        :return: the time it takes for the node to change the channel to which it listens
        :rtype: pandas.Timedelta
        """
        return self.__channel_switching_time

    @property
    def node_group(self):
        """
        :return: the group to which the node belongs
        :rtype: ieee802154.node_group.NodeGroup
        """
        return self.__node_group

    @property
    def mac_address(self):
        """
        Return the mac address of the node
        :return: the mac address of the node
        :rtype: str
        """
        return self.__mac_address

    def distance_from_node(self, node):
        """
        the distance from the given node
        :param node: the node from which the distance will be calculated
        :type node: Node
        :return: the distance from the given node
        :rtype: float
        """
        return self.__distance(self.position, node.position)

    def distance_from_point(self, point):
        """
        the distance from the given point
        :param point: the point from which the distance will be calculated
        :type point: (float|int, float|int)
        :return: the distance from the given point
        :rtype: float
        """
        return self.__distance(self.position, point)

    def __new_move(self, start_time=None):
        """
        Defines the next move of the node
        """
        MIN_SPEED = 0.1  # m/s
        MAX_SPEED = 5  # m/s
        area_dimensions = self.__node_group.properties.area_dimensions
        start_pos = self.__move.get("end_pos", self.__initial_position)

        while True:
            end_pos = (self.__randgen.random() * area_dimensions[0], self.__randgen.random() * area_dimensions[1])
            if start_pos != end_pos:
                break

        speed = self.__randgen.random() * MAX_SPEED

        if speed < MIN_SPEED:
            speed = MIN_SPEED

        if start_time is None:
            start_time = self.__node_group.time

        self.__move = {
            "start_pos": start_pos,
            "start_t": start_time,
            "end_pos": end_pos,
            "speed": speed
        }
