#!/usr/bin/env python3

'''

Generates plot files for 
- prefix reachability, 
- preferred route interval, and 
- prefix withdrawn interval 
for two experiments.

'''

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from argparse import ArgumentParser
from filenames_directories import quantiles_with_clock_filename

from scipy.interpolate import interp1d

clock_error = 'clock_p_90'


# compute and normalize to 1 (to report fractions of...)
def F_do_normalize(x,data):
    return float(len(data[data <= x]))*1.0/len(data)

# def do not normalize
def F_do_not_normalize(x,data):
    return float(len(data[data <= x]))


# Adds to a dataframe columns with quantiles corrected with clock offset error
def add_clock_info(qdf: pd.DataFrame) -> pd.DataFrame:

    qdf['minA_q50_minus_clock'] = qdf['minA_q50_UP'] - qdf[clock_error]
    # set to 0 negative values, otherwise, same as before
    qdf['minA_q50_minus_clock']= qdf['minA_q50_minus_clock'].where(qdf['minA_q50_minus_clock'] > 0, 0)
    qdf['minA_q50_plus_clock'] = qdf['minA_q50_UP'] + qdf[clock_error]
    # Rename for consistent processing (get rit of '_UP' in the name)
    qdf['minA_q50'] = qdf['minA_q50_UP']


    ###########
    qdf['maxA_q50_minus_clock'] = qdf['maxA_q50_UP'] - qdf[clock_error]
    # set to 0 negative values, otherwise, same as before
    qdf['maxA_q50_minus_clock']= qdf['maxA_q50_minus_clock'].where(qdf['maxA_q50_minus_clock'] > 0, 0)
    qdf['maxA_q50_plus_clock'] = qdf['maxA_q50_UP'] + qdf[clock_error]
    qdf['maxA_q50'] = qdf['maxA_q50_UP'] 


    ###########
    qdf['maxW_q50_minus_clock'] = qdf['maxW_q50_DOWN'] - qdf[clock_error]
    # set to 0 negative values, otherwise, same as before
    qdf['maxW_q50_minus_clock']= qdf['maxW_q50_minus_clock'].where(qdf['maxW_q50_minus_clock'] > 0, 0)
    qdf['maxW_q50_plus_clock'] = qdf['maxW_q50_DOWN'] + qdf[clock_error]
    qdf['maxW_q50'] = qdf['maxW_q50_DOWN']

    return qdf


# Only v4 beacons remain
def only_ipv4(qdf: pd.DataFrame) -> pd.DataFrame:
    qdf = qdf[~qdf.prefix.str.contains(':')]
    return qdf


# Only v6 beacons remain
def only_ipv6(qdf: pd.DataFrame) -> pd.DataFrame:
    qdf = qdf[qdf.prefix.str.contains(':')]
    return qdf


# plotCDF(qdf1_4['maxA_q50_UP'], '-', 'red', 'IPv6...')
def plotCDF(column, linestyle, color, label):
    vF = np.vectorize(F_do_normalize, excluded=['data'])

    val = column.sort_values().values
    # as many points as x values
    x = np.arange(0, val[-1], 1/val[-1])
    plt.plot(x, vF(x=x, data=val), linestyle=linestyle, color=color, label=label)


#./plot_quantiles_with_clock.py 20121001_30d 20181001_30d
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("exp_name1")
    # second experiment
    parser.add_argument("exp_name2")

    args= parser.parse_args()
    exp_name1 = args.exp_name1
    exp_name2 = args.exp_name2
    
    fn1 = quantiles_with_clock_filename(exp_name1)
    qdf1 = pd.read_csv(fn1)

    fn2 = quantiles_with_clock_filename(exp_name2)
    qdf2 = pd.read_csv(fn2)

    min_events =  45
    
    qdf1 = qdf1[(qdf1['count_UP_events'] > min_events) & (qdf1['count_DOWN_events'] > min_events)]
    qdf1 = add_clock_info(qdf1)
    qdf1_4 = only_ipv4(qdf1)
    qdf1_6 = only_ipv6(qdf1)

    qdf2 = qdf2[(qdf2['count_UP_events'] > min_events) & (qdf2['count_DOWN_events'] > min_events)]
    qdf2 = add_clock_info(qdf2)
    qdf2_4 = only_ipv4(qdf2)
    qdf2_6 = only_ipv6(qdf2)


    plt.figure(figsize=(5,3.4))


    # To transform to x=values, y=fraction of pairs
    vF = np.vectorize(F_do_normalize, excluded=['data'])


    # minA1_4 = qdf1_4['minA_q50_UP'].sort_values().values
    minA_minus1_4 = qdf1_4['minA_q50_minus_clock'].sort_values().values
    minA_plus1_4 = qdf1_4['minA_q50_plus_clock'].sort_values().values

    # To use plt.fill_between we must provide y_min(x) and y_max(x) for the same x.
    # To do so, we 
    # (1) transform to cdf format each of the series (minus and plus), with vF
    # (2) use np.interpld to generate an function that can interpolate
    # (3) use fill_between with this function, for the same x values
    # This is only needed for fill_between, the rest is easier

    x1_4 = np.arange(0, minA_minus1_4[-1], 1/minA_minus1_4[-1])
    f_minus1_4 = interp1d(x1_4, vF(x=x1_4, data=minA_minus1_4))
    f_plus1_4 = interp1d(x1_4, vF(x=x1_4, data=minA_plus1_4))

    plt.fill_between(x1_4, f_minus1_4(x1_4), f_plus1_4(x1_4),  facecolor='powderblue', label='Clock offset error, IPv4, {}'.format(exp_name1[:4]))

    plotCDF(qdf1_4['minA_q50_UP'], '-', 'blue', 'IPv4, {}'.format(exp_name1[:4]))

    minA_minus2_4 = qdf2_4['minA_q50_minus_clock'].sort_values().values
    minA_plus2_4 = qdf2_4['minA_q50_plus_clock'].sort_values().values

    x2_4 = np.arange(0, minA_minus2_4[-1], 1/minA_minus2_4[-1])
    f_minus2_4 = interp1d(x2_4, vF(x=x2_4, data=minA_minus2_4))
    f_plus2_4 = interp1d(x2_4, vF(x=x2_4, data=minA_plus2_4))

    plt.fill_between(x2_4, f_minus2_4(x2_4), f_plus2_4(x2_4), facecolor='wheat', label='Clock offset error, IPv4, {}'.format(exp_name2[:4]))
    plotCDF(qdf2_4['minA_q50_UP'], 'dashed', 'blue', 'IPv4, {}'.format(exp_name2[:4]))

    
    #########

    plotCDF(qdf1_6['minA_q50_UP'], '-', 'red', 'IPv6, {}'.format(exp_name1[:4]))
    plotCDF(qdf2_6['minA_q50_UP'], 'dashed', 'red', 'IPv6, {}'.format(exp_name2[:4]))


    # set x ticks every 15s
    maxx=plt.xlim()
    plt.xticks(np.arange(0, maxx[1]+1, 15.0))

    # Plot horizontal line at 15s and 30s
    plt.axvline(15, linestyle=(0,(5,10)), color='lightgrey', linewidth=0.9)
    plt.axvline(30, linestyle=(0,(5,10)), color='lightgrey', linewidth=0.9)


    plt.legend(loc='best')
    plt.xlabel('Seconds')
    plt.ylabel('CDF monitor/beacon pairs')
    plt.tight_layout()
    #plt.show()
    plt.savefig(exp_name1 + '_' + exp_name2 + '_minA_with_clock.eps', format='eps', dpi=2400)
    plt.clf()

    
    #############

    plt.figure(figsize=(5,2.7))


    # Plot horizontal line at 15s and 30s
    plt.axvline(15, linestyle=(0,(5,10)), color='lightgrey', linewidth=0.7)
    plt.axvline(30, linestyle=(0,(5,10)), color='lightgrey', linewidth=0.7)
    plt.axvline(45, linestyle=(0,(5,10)), color='lightgrey', linewidth=0.7)

    plotCDF(qdf1_4['maxA_q50_UP'], '-', 'blue', label='IPv4, {}'.format(exp_name1[:4]))
    plotCDF(qdf1_6['maxA_q50_UP'], '-', 'red', label='IPv6, {}'.format(exp_name1[:4]))
    plotCDF(qdf2_4['maxA_q50_UP'], 'dashed', 'blue', label='IPv4, {}'.format(exp_name2[:4]))
    plotCDF(qdf2_6['maxA_q50_UP'], 'dashed', 'red', label='IPv6, {}'.format(exp_name2[:4]))
    

    # set y ticks every 30s
    maxx=plt.xlim()
    plt.xticks(np.arange(0, maxx[1]+1, 30.0))

    plt.legend(loc='best')
    plt.xlabel('Seconds')
    plt.ylabel('CDF monitor/beacon pairs')
    plt.tight_layout()
    #plt.show()
    plt.savefig(exp_name1 + '_' + exp_name2 + '_maxA.eps', format='eps', dpi=2400)
    plt.clf()


    ###################################


    # print(qdf2_4[qdf2_4['maxW_q50_DOWN']> 300][['monitor_ip','prefix', 'maxW_q50_DOWN']])
    # print(qdf2_4[qdf2_4['maxW_q50_DOWN']> 300]['monitor_ip'].unique())
    # print(qdf2_6[qdf2_6['maxW_q50_DOWN']> 300][['monitor_ip','prefix', 'maxW_q50_DOWN']])
    # print(qdf2_6[qdf2_6['maxW_q50_DOWN']> 300]['monitor_ip'].unique())

    
    # Remove data for outliers
    qdf2_4 = qdf2_4[qdf2_4['monitor_ip'] != '187.16.223.117']
    qdf2_6 = qdf2_6[qdf2_6['monitor_ip'] != '2001:12f8::223:117']
    print('--- REMOVING 223.117 and ::223:117 for second experiment')
    qdf2_4 = qdf2_4[qdf2_4['monitor_ip'] != '193.0.0.56']
    qdf2_6 = qdf2_6[qdf2_6['monitor_ip'] != '193.0.0.56']
    print('--- REMOVING 193.0.0.56 for second experiment')



    plotCDF(qdf1_4['maxW_q50_DOWN'], '-', 'blue', 'IPv4, {}'.format(exp_name1[:4]))
    plotCDF(qdf1_6['maxW_q50_DOWN'], '-', 'red', 'IPv6, {}'.format(exp_name1[:4]))
    plotCDF(qdf2_4['maxW_q50_DOWN'], 'dashed', 'blue', 'IPv4, {}'.format(exp_name2[:4]))
    plotCDF(qdf2_6['maxW_q50_DOWN'], 'dashed', 'red', 'IPv6, {}'.format(exp_name2[:4]))

    
    plt.axvline(60, linestyle=(0,(5,10)), color='darkgrey', linewidth=0.7)
    plt.axvline(30, linestyle=(0,(5,10)), color='darkgrey', linewidth=0.7)

    #print('plotting only to an xmax=280')
    plt.xlim(right=280)

    # set y ticks every 60s
    maxx=plt.xlim()
    plt.xticks(np.arange(0, maxx[1]+1, 60.0))

    plt.legend(loc='best')
    plt.xlabel('Seconds')
    plt.ylabel('CDF monitor/beacon pairs')
    plt.tight_layout()
    #plt.show()
    plt.savefig(exp_name1 + '_' + exp_name2 + '_maxW_no_outliers.eps', format='eps', dpi=2400)
