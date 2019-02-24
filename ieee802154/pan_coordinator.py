import ieee802154
from ieee802154.node import Node, NodeType, NotValidNodeConfigError


class PANCoordinatorCreationError(Exception):
    pass


class PANCoordinator(Node):
    def __init__(self, id, position, tx_power, radio_sensitivity, boot_time, channel_switching_time, node_group):
        """
        :param id: the identifier of the pan coordinator
        :type id: int
        :param position: the position of the pan coordinator expressed in cartesian dimensions
        :type position: (int | float, int | float)
        :param tx_power the transmission power of the pan coordinator, in dBm
        :type int
        :param radio_sensitivity the radio sensitivity of the pan coordinator, in dBm
        :type int
        :param boot_time: the time it takes for the pan coordinator to be ready to operate after the power has been
        turned on
        :type boot_time: pandas.Timedelta
        :param channel_switching_time: the time it takes the node to change channel
        :type channel_switching_time: pandas.Timedelta
        :param node_group: the group to which the pan coordinator belongs
        :type node_group: ieee802154.node_group.NodeGroup
        :raise NotValidNodeConfigError: : if at least one of the specified parameters is not valid
        :raise PANCoordinatorCreationError: if there is already a PAN coordinator in the specified group
        """
        if not isinstance(node_group, ieee802154.node_group.NodeGroup):
            raise NotValidNodeConfigError("The node_group must be an instance of {}.{}".format(
                ieee802154.node_group.NodeGroup.__module__,
                ieee802154.node_group.NodeGroup.__name__))

        if node_group.pan_coordinator is None:
            super().__init__(id, position, False, NodeType.FFD, tx_power, radio_sensitivity, boot_time,
                             channel_switching_time, node_group)

            node_group._NodeGroup__set_pan_coordinator(self)
        else:
            raise PANCoordinatorCreationError(
                "PAN coordinator creation failed. There is already a PAN coordinator in the specified group")
