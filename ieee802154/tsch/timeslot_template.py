from pandas import Timedelta


class NotValidTimeslotTemplateError(Exception):
    pass


class TimeslotTemplate:
    def __init__(self, attributes):
        """
        Creates a TimeslotTemplate object.
        :param attributes: A dictionary including the timeslot template attributes.
        The timeslot template attributes, as described to the standard (IEEE Std 802.15.4-2015), are the following:
        - macTsCcaOffset: The time between the beginning of timeslot and start of CCA operation, in μs.
        - macTsCca: Duration of CCA, in μs.
        - macTsTxOffset: The time between the beginning of the timeslot and the start of frame transmission, in μs.
        - macTsRxOffset: Beginning of the timeslot to when the receiver shall be listening, in μs.
        - macTsRxAckDelay: End of frame to when the transmitter shall listen for acknowledgment, in μs.
        - macTsTxAckDelay: End of frame to start of acknowledgment, in μs.
        - macTsRxWait: The time to wait for start of frame, in μs.
        - macTsRxTx: Transmit to Receive turnaround, in μs.
        - macTsMaxAck: Transmission time to send an acknowledgment, in μs.
        - macTsMaxTx: Transmission time to send the maximum length frame, in μs.
        - macTsTimeslotLength: The total length of the timeslot including any unused time after frame transmission and
        acknowledgment, in μs.
        - macTsAckWait: The minimum time to wait for the start of an acknowledgment in μs.
        The attributes should be specified as integers in the range of 0–65535.
        :type attributes: dict[str, int]
        :raise KeyError: if one of the above attributes is not specified
        :raise NotValidTimeslotTemplateError: if at least one of the specified attributes is not valid
        """
        if len(attributes) > 12:
            raise NotValidTimeslotTemplateError("Unknown attributes were given")

        self.__mac_ts_cca_offset = Timedelta(attributes["macTsCcaOffset"], unit="us")
        self.__mac_ts_cca = Timedelta(attributes["macTsCca"], unit="us")
        self.__mac_ts_rx_tx = Timedelta(attributes["macTsRxTx"], unit="us")
        self.__mac_ts_tx_offset = Timedelta(attributes["macTsTxOffset"], unit="us")
        self.__mac_ts_max_tx = Timedelta(attributes["macTsMaxTx"], unit="us")
        self.__mac_ts_rx_offset = Timedelta(attributes["macTsRxOffset"], unit="us")
        self.__mac_ts_rx_wait = Timedelta(attributes["macTsRxWait"], unit="us")
        self.__mac_ts_rx_ack_delay = Timedelta(attributes["macTsRxAckDelay"], unit="us")
        self.__mac_ts_tx_ack_delay = Timedelta(attributes["macTsTxAckDelay"], unit="us")
        self.__mac_ts_ack_wait = Timedelta(attributes["macTsAckWait"], unit="us")
        self.__mac_ts_max_ack = Timedelta(attributes["macTsMaxAck"], unit="us")
        self.__mac_ts_timeslot_length = Timedelta(attributes["macTsTimeslotLength"], unit="us")

        for name, value in attributes.items():
            if not isinstance(value, int) or value < 0 or value > 65535:
                raise NotValidTimeslotTemplateError(
                    "The attribute {} must have a value in the range 0-65535.".format(name))

        if (self.__mac_ts_tx_offset != self.__mac_ts_cca_offset + self.__mac_ts_cca + self.__mac_ts_rx_tx
                or self.__mac_ts_tx_offset != self.__mac_ts_rx_offset + self.__mac_ts_rx_wait / 2
                or self.__mac_ts_rx_ack_delay > self.__mac_ts_tx_ack_delay
                or self.__mac_ts_rx_ack_delay + self.__mac_ts_ack_wait <= self.__mac_ts_tx_ack_delay
                or self.__mac_ts_tx_offset + self.__mac_ts_max_tx + self.__mac_ts_rx_ack_delay +
                self.__mac_ts_ack_wait > self.__mac_ts_timeslot_length
                or self.__mac_ts_tx_offset + self.__mac_ts_max_tx + self.__mac_ts_tx_ack_delay +
                self.__mac_ts_max_ack > self.__mac_ts_timeslot_length
                or self.__mac_ts_rx_offset + self.__mac_ts_rx_wait + self.__mac_ts_max_tx +
                self.__mac_ts_tx_ack_delay + self.__mac_ts_max_ack > self.__mac_ts_timeslot_length +
                self.__mac_ts_cca_offset
                or self.__mac_ts_rx_wait / 2 > self.__mac_ts_rx_offset + self.__mac_ts_timeslot_length -
                self.__mac_ts_tx_offset - self.__mac_ts_max_tx - self.__mac_ts_tx_ack_delay - self.__mac_ts_max_ack):
            raise NotValidTimeslotTemplateError("The timeslot template is not valid")

    @property
    def mac_ts_cca_offset(self):
        """
        The time between the beginning of timeslot and start of CCA operation.
        :return: the value of the attribute macTsCcaOffset
        :rtype: pandas.Timedelta
        """
        return self.__mac_ts_cca_offset

    @property
    def mac_ts_cca(self):
        """
        Duration of CCA.
        :return: the value of the attribute macTsCca
        :rtype: pandas.Timedelta
        """
        return self.__mac_ts_cca

    @property
    def mac_ts_rx_tx(self):
        """
        Transmit to Receive turnaround.
        :return: the value of the attribute macTsRxTx
        :rtype: pandas.Timedelta
        """
        return self.__mac_ts_rx_tx

    @property
    def mac_ts_tx_offset(self):
        """
        The time between the beginning of the timeslot and the start of frame transmission.
        :return: the value of the attribute macTsTxOffset
        :rtype: pandas.Timedelta
        """
        return self.__mac_ts_tx_offset

    @property
    def mac_ts_max_tx(self):
        """
        Transmission time to send the maximum length frame.
        :return: the value of the attribute macTsMaxTx
        :rtype: pandas.Timedelta
        """
        return self.__mac_ts_max_tx

    @property
    def mac_ts_rx_offset(self):
        """
        Beginning of the timeslot to when the receiver shall be listening.
        :return: the value of the attribute macTsRxOffset
        :rtype: pandas.Timedelta
        """
        return self.__mac_ts_rx_offset

    @property
    def mac_ts_rx_wait(self):
        """
        The time to wait for start of frame.
        :return: the value of the attribute macTsRxWait
        :rtype: pandas.Timedelta
        """
        return self.__mac_ts_rx_wait

    @property
    def mac_ts_rx_ack_delay(self):
        """
        End of frame to when the transmitter shall listen for acknowledgment.
        :return: the value of the attribute macTsRxAckDelay
        :rtype: pandas.Timedelta
        """
        return self.__mac_ts_rx_ack_delay

    @property
    def mac_ts_tx_ack_delay(self):
        """
        End of frame to start of acknowledgment.
        :return: the value of the attribute macTsTxAckDelay
        :rtype: pandas.Timedelta
        """
        return self.__mac_ts_tx_ack_delay

    @property
    def mac_ts_ack_wait(self):
        """
        The minimum time to wait for the start of an acknowledgment.
        :return: the value of the attribute macTsAckWait
        :rtype: pandas.Timedelta
        """
        return self.__mac_ts_ack_wait

    @property
    def mac_ts_max_ack(self):
        """
        Transmission time to send an acknowledgment.
        :return: the value of the attribute macTsMaxAck
        :rtype: pandas.Timedelta
        """
        return self.__mac_ts_max_ack

    @property
    def mac_ts_timeslot_length(self):
        """
        The total length of the timeslot including any unused time after frame transmission and acknowledgment.
        :return: the value of the attribute macTsTimeslotLength
        :rtype: pandas.Timedelta
        """
        return self.__mac_ts_timeslot_length


defaultTimeslotTemplateFor2450MHzBand = TimeslotTemplate({  # according to the IEEE Std 802.15.4-2015 standard
    "macTsCcaOffset": 1800,
    "macTsCca": 128,
    "macTsTxOffset": 2120,
    "macTsRxOffset": 1020,
    "macTsRxAckDelay": 800,
    "macTsTxAckDelay": 1000,
    "macTsRxWait": 2200,
    "macTsRxTx": 192,
    "macTsMaxAck": 2400,
    "macTsMaxTx": 4256,
    "macTsTimeslotLength": 10000,
    "macTsAckWait": 400
})
