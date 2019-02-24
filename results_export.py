import csv
import os
import sqlite3
import bootstrapped.bootstrap as bs
import bootstrapped.stats_functions as bs_stats
import numpy

from sim_for_fixed_joining_node import Scenario
from ieee802154.tsch.joining_phase_simulator import EBSchedulingMethod

simulations_with_fixed_nodes = [
    (EBSchedulingMethod.CFASV, Scenario.ANY, False), (EBSchedulingMethod.CFASV, Scenario.ANY, True),
    (EBSchedulingMethod.CFASH, Scenario.ANY, False), (EBSchedulingMethod.CFASH, Scenario.ANY, True),
    (EBSchedulingMethod.ECFASV, Scenario.ONE_HOP, False), (EBSchedulingMethod.ECFASV, Scenario.ONE_HOP, True),
    (EBSchedulingMethod.ECFASV, Scenario.TWO_HOPS, False), (EBSchedulingMethod.ECFASV, Scenario.TWO_HOPS, True),
    (EBSchedulingMethod.ECFASH, Scenario.ONE_HOP, False), (EBSchedulingMethod.ECFASH, Scenario.ONE_HOP, True),
    (EBSchedulingMethod.ECFASH, Scenario.TWO_HOPS, False), (EBSchedulingMethod.ECFASH, Scenario.TWO_HOPS, True),
    (EBSchedulingMethod.ECV, Scenario.ONE_HOP), (EBSchedulingMethod.ECV, Scenario.TWO_HOPS),
    (EBSchedulingMethod.ECH, Scenario.ONE_HOP),
    (EBSchedulingMethod.ECH, Scenario.TWO_HOPS),
    (EBSchedulingMethod.Minimal6TiSCH, Scenario.ANY)
]

simulations_with_mobile_node = [
    (EBSchedulingMethod.CFASV, False), (EBSchedulingMethod.CFASV, True),
    (EBSchedulingMethod.CFASH, False), (EBSchedulingMethod.CFASH, True),
    (EBSchedulingMethod.ECFASV, False), (EBSchedulingMethod.ECFASV, True),
    (EBSchedulingMethod.ECFASH, False), (EBSchedulingMethod.ECFASH, True),
    (EBSchedulingMethod.ECV,), (EBSchedulingMethod.ECH,),
    (EBSchedulingMethod.Minimal6TiSCH,)
]

os.makedirs(os.path.join("filtered_statistics", "fixed_joining_node"), exist_ok=True)
os.makedirs(os.path.join("filtered_statistics", "mobile_joining_node"), exist_ok=True)

for sim in simulations_with_fixed_nodes:
    scheduling_method = sim[0]
    selected_scenario = sim[1]
    atp_enabled = sim[2] if len(sim) == 3 else False

    db_name = "{}{}{}".format(scheduling_method.name, ("_with_ATP" if atp_enabled else ""),
                              ("_{}".format(selected_scenario.name) if selected_scenario is not Scenario.ANY else ""))

    db_conn = sqlite3.connect(os.path.join("statistics", "fixed_joining_node", "{}.db".format(db_name)))
    db_conn.row_factory = sqlite3.Row
    c = db_conn.cursor()
    export_file = os.path.join("filtered_statistics", "fixed_joining_node", "{}.csv".format(db_name))

    with open(export_file, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        if scheduling_method not in {EBSchedulingMethod.ECV, EBSchedulingMethod.ECH}:
            csv_writer.writerow(["Neighboring Advertisers", "Joining Time (s)"])
            csv_writer.writerow([""] + ["AVG", "CI_LL", "CI_UL"])
        else:
            csv_writer.writerow(["Neighboring Advertisers", "Joining Time (s)", "", "", "Sensed Slots", "", "",
                                 "EB Scheduling Delay"])
            csv_writer.writerow([""] + ["AVG", "CI_LL", "CI_UL"] * 3)

        for advertisers in range(1, 11):
            record = [advertisers]

            if scheduling_method not in {EBSchedulingMethod.ECV, EBSchedulingMethod.ECH}:
                joining_time_samples = [row[0] for row in c.execute(
                    '''SELECT time FROM joining_time_samples WHERE neighboring_advertisers=?''',
                    (advertisers,))]

                res = bs.bootstrap(numpy.asarray(joining_time_samples), stat_func=bs_stats.mean, num_iterations=1000)
                record += [res.value, res.lower_bound, res.upper_bound]

            else:
                joining_time_samples = []
                sensed_slots_samples = []
                eb_scheduling_delay_samples = []
                for row in c.execute('''SELECT * FROM joining_time_samples WHERE neighboring_advertisers=?''',
                                     (advertisers,)):
                    joining_time_samples.append(row["time"])
                    sensed_slots_samples.append(row["num_adv_slots_sensed"])
                    eb_scheduling_delay_samples.append(row["eb_scheduling_delay"])

                res = bs.bootstrap(numpy.asarray(joining_time_samples), stat_func=bs_stats.mean, num_iterations=1000)
                record += [res.value, res.lower_bound, res.upper_bound]
                res = bs.bootstrap(numpy.asarray(sensed_slots_samples), stat_func=bs_stats.mean, num_iterations=1000)
                record += [res.value, res.lower_bound, res.upper_bound]
                res = bs.bootstrap(numpy.asarray(eb_scheduling_delay_samples), stat_func=bs_stats.mean,
                                   num_iterations=1000)
                record += [res.value, res.lower_bound, res.upper_bound]

            csv_writer.writerow(record)

for sim in simulations_with_mobile_node:
    scheduling_method = sim[0]
    atp_enabled = sim[1] if len(sim) == 2 else False
    db_name = "{}{}".format(scheduling_method.name, ("_with_ATP" if atp_enabled else ""))
    db_conn = sqlite3.connect(os.path.join("statistics", "mobile_joining_node", "{}.db".format(db_name)))
    db_conn.row_factory = sqlite3.Row
    c = db_conn.cursor()
    export_file = os.path.join("filtered_statistics", "mobile_joining_node", "{}.csv".format(db_name))

    with open(export_file, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(["Advertisers", "Joining Time (s)", "", ""])
        csv_writer.writerow([""] + ["AVG", "CI_LL", "CI_UL"])

        for num_advertisers in range(10, 71, 10):  # excluding PAN coordinator
            record = [num_advertisers]

            mobile_node_joining_time_samples = []
            for row in c.execute('''SELECT * FROM mobile_node_joining_time_samples WHERE advertisers=?''',
                                 (num_advertisers,)):
                mobile_node_joining_time_samples.append(row["time"])

            res = bs.bootstrap(numpy.asarray(mobile_node_joining_time_samples), stat_func=bs_stats.mean,
                               num_iterations=1000)
            record += [res.value, res.lower_bound, res.upper_bound]
            csv_writer.writerow(record)
