import random

from pandas import Timedelta
from ieee802154.node import Node, NodeType


class NotValidGroupProperties(Exception):
    pass


class NodeGroupProperties:
    def __init__(self, data_rate, area_dimensions):
        """
        :param data_rate: the data rate, in bps
        :type data_rate:  int
        :param area_dimensions: the area dimensions, in meters
        :type area_dimensions: (int | float, int | float)
        :raise NotValidGroupProperties if one of the properties is not valid
        """
        if not isinstance(data_rate, int) or data_rate <= 0:
            raise NotValidGroupProperties("The parameter data_rate must be a positive integer")
        elif not isinstance(area_dimensions, tuple) or (
                not isinstance(area_dimensions[0], (float, int)) or not isinstance(area_dimensions[1], (float, int))):
            raise NotValidGroupProperties(
                "The parameter area_dimensions must be expressed in cartesian dimensions (x,y)")

        self.__data_rate = data_rate
        self.__area_dimensions = area_dimensions

    @property
    def data_rate(self):
        """
        :return: the data rate, in bps
        :rtype: int
        """
        return self.__data_rate

    @property
    def area_dimensions(self):
        """
        :return: the area dimensions
        :rtype: (int | float, int | float)
        """
        return self.__area_dimensions


class NodeGroup:
    """
    This class represents a group of nodes.
    Note that an object of this class must be used by only one JoiningPhaseSimulator (see the JoiningPhaseSimulator
    class).
    """

    def __init__(self, properties):
        """
        :param properties: the properties of the group
        :type properties: NodeGroupProperties
        """
        self.__nodes = []
        self.__pan_coordinator = None
        self.__properties = properties
        self.__time = Timedelta(0)
        self.__num_ffds = 0
        self.__macs_in_use = []

    def __iter__(self):
        """
        :return: an iterator for the group's nodes
        :rtype: collections.abc.Iterator[Node]
        """
        return iter(self.__nodes)

    # The following two private functions are used by the friend class Node
    def __add_node(self, node):
        self.__nodes.append(node)
        self.___assign_mac_addr(node)
        if node.type is NodeType.FFD:
            self.__num_ffds += 1

    def __set_pan_coordinator(self, pan_coordinator):
        self.__pan_coordinator = pan_coordinator

    @property
    def pan_coordinator(self):
        """
        :return: the pan coordinator
        :rtype: ieee802154e.pan_coordinator.PANCoordinator
        """
        return self.__pan_coordinator

    @property
    def properties(self):
        """
        :return: the group's properties
        :rtype:  NodeGroupProperties
        """
        return self.__properties

    @property
    def size(self):
        """
        :return: the number of nodes in the group
        :rtype: int
        """
        return len(self.__nodes)

    @property
    def num_ffds(self):
        """
        :return: the number of Full Function Devices in the group
        :rtype: int
        """
        return self.__num_ffds

    @property
    def time(self):
        """
        Returns the (reference) time of the network group.
        The time is automatically set by the JoiningPhaseSimulator using the corresponding node group object (i.e. we
        consider that the class JoiningPhaseSimulator is a friend class that has access to set the time of the node
        group).
        :return: The time of the network group
        :rtype: Timedelta
        """
        return self.__time

    def ___assign_mac_addr(self, node):
        while True:
            random_mac = [0x00, 0x8c, 0xfa, random.randint(0x00, 0xff), random.randint(0x00, 0xff),
                          random.randint(0x00, 0xff)]

            random_mac = '-'.join(map(lambda x: "%02x" % x, random_mac))
            if random_mac not in self.__macs_in_use:
                break

        node._Node__mac_address = random_mac
        self.__macs_in_use.append(random_mac)

