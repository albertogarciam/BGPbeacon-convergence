#!/usr/bin/env python3

'''
Plots clock offset error.
Each line represents the computed upper bound for the clock offset error 
for different collector pairs for a single experiment. 
Requires a minimum number of events per pair, 45 (out of a total of 90
for a whole 30 day experiment).
Prepared to plot experiment results ranging from 2011 to 2018.

Input: 'per_experiment_clock_synch_DOWN.csv' file for each experiment.
'''

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from filenames_directories import per_experiment_clock_synch_filename

min_event_count=45

def plot_error(exp_name, color, linestyle):
    # Down
    fn = per_experiment_clock_synch_filename(exp_name, False, True)
    qdf = pd.read_csv(fn)

    label = exp_name[:4] 
    few_event_pair_count = len(qdf[qdf['event_count']< min_event_count])
    if  few_event_pair_count> 0:
        print('Number of events with few event count (excluded from plot): ', few_event_pair_count)
        qdf = qdf[qdf['event_count']>= min_event_count]
    print('Computed pairs for year {}: {}'.format(label, str(len(qdf))))

    x = np.arange(0, len(qdf))
    plt.plot(x, qdf['p_90'].sort_values(), color=color, label=label, linestyle=linestyle)


# ./plot_error_per_collector_pair.py
if __name__ == "__main__":

    plt.figure(figsize=(5, 3.2))
    plt.rcParams['axes.grid'] = True

    plot_error('20111001_30d', 'green', '-')
    plot_error('20121001_30d', 'red', '-')
    plot_error('20131001_30d', 'blue', '-')
    plot_error('20141001_30d', 'green', '--')
    plot_error('20151001_30d', 'blue', '--')
    plot_error('20161001_30d', 'black', '-')
    plot_error('20171001_30d', 'black', '--')
    plot_error('20181001_30d', 'red', '--')

    plt.xlim(-2, 90)
    plt.xlabel('Collector pairs')
    plt.ylabel('Seconds')

    plt.legend(loc='best')
    plt.tight_layout()

    plt.savefig('clock_per_collector_pair.eps', format='eps', dpi=2400)
    print('Generated: clock_per_collector_pair.eps')