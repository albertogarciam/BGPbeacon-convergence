#!/usr/bin/env python3

''' 
Parameters defining the experiments:
- Data directories
- List of beacons, anchors, collector names
'''

# Set here the place for your data.
# Tries first 'result_directory'; if not available, tries 
# 'alternative_result_directory'
# Intended to be used with different (remote) mounting points for 'alternative...'
result_directory= '/srv/agarcia/beacon_convergence/'
alternative_result_directory= '/media/srv_zompopo/beacon_convergence/'

# ((tuple of beacons, ...), corresponding_anchor, name)
# https://www.ripe.net/analyse/internet-measurements/routing-information-service-ris/current-ris-routing-beacons
beacons_v4_2009 = (
    (('84.205.64.0/24', ),  '84.205.80.0/24', 'RRC00'),
    (('84.205.65.0/24', ),  '84.205.81.0/24', 'RRC01'),
    # exclude 84.205.67.0/24, RRC03: Colin Petrie indicates in a private mail that 
    # this beacon has followed a 20 min on/off cycle for long time
    (('84.205.68.0/24', ),	'84.205.84.0/24', 'RRC04'),
    (('84.205.69.0/24', ),	'84.205.85.0/24', 'RRC05'),
    (('84.205.70.0/24', ),	'84.205.86.0/24', 'RRC06'),
    (('84.205.71.0/24', ),	'84.205.87.0/24', 'RRC07'),
    (('84.205.74.0/24', ),	'84.205.90.0/24', 'RRC10'),
    (('84.205.75.0/24', ),	'84.205.91.0/24', 'RRC11'),
    (('84.205.76.0/24', ),	'84.205.92.0/24', 'RRC12'),
    (('84.205.77.0/24', ),	'84.205.93.0/24', 'RRC13'),
    (('84.205.78.0/24', ),	'84.205.94.0/24', 'RRC14'),
    (('84.205.79.0/24', ),	'84.205.95.0/24', 'RRC15'),
    # No data for rrc16 collector from 2012.03 and 2016.01
    #(('84.205.73.0/24', ),	'84.205.89.0/24', 'RRC16')
)

beacons_v6_2009 = (
    (('2001:7fb:fe00::/48', ),'2001:7fb:ff00::/48',	'RRC00'),
    (('2001:7fb:fe01::/48', ),'2001:7fb:ff01::/48',	'RRC01'),
    # Exclude v6 beacon for rrc03 from list
    # (('2001:7fb:fe03::/48', ),'2001:7fb:ff03::/48',	'RRC03'),
    (('2001:7fb:fe04::/48', ),'2001:7fb:ff04::/48',	'RRC04'),
    (('2001:7fb:fe05::/48', ),'2001:7fb:ff05::/48',	'RRC05'),
    (('2001:7fb:fe06::/48', ),'2001:7fb:ff06::/48',	'RRC06'),
    (('2001:7fb:fe07::/48', ),'2001:7fb:ff07::/48',	'RRC07'),
    (('2001:7fb:fe0a::/48', ),'2001:7fb:ff0a::/48',	'RRC10'),
    (('2001:7fb:fe0b::/48', ),'2001:7fb:ff0b::/48',	'RRC11'),
    (('2001:7fb:fe0c::/48', ),'2001:7fb:ff0c::/48',	'RRC12'),
    (('2001:7fb:fe0d::/48', ),'2001:7fb:ff0d::/48',	'RRC13'),
    (('2001:7fb:fe0e::/48', ),'2001:7fb:ff0e::/48',	'RRC14'),
    (('2001:7fb:fe0f::/48', ),'2001:7fb:ff0f::/48',	'RRC15'),
    #(('2001:7fb:fe10::/48', ),'2001:7fb:ff10::/48',	'RRC16'),
)

# to be imported in other modules:
beacons_2009 = beacons_v4_2009 + beacons_v6_2009
collector_list_2009 = [x[2] for x in beacons_2009]


experiments = {
    '20111001_30d':{
        'init_day': '20111001', 
        'end_day': '20111030', 
    },
        '20131001_30d':{
        'init_day': '20131001', 
        'end_day': '20131030', 
    },
    '20141001_30d':{
        'init_day': '20141001', 
        'end_day': '20141030', 
    },
    '20161001_30d':{
        'init_day': '20161001', 
        'end_day': '20161030', 
    },
    '20151001_30d':{
        'init_day': '20151001', 
        'end_day': '20151030', 
    },
    '20171001_30d':{
        'init_day': '20171001', 
        'end_day': '20171030', 
    },
    '20091001_30d':{
        'init_day': '20091001', 
        'end_day': '20091030', 
    },
    '20121001_30d':{
        'init_day': '20121001', 
        'end_day': '20121030', 
    },
    '20151001_30d':{
        'init_day': '20151001', 
        'end_day': '20151030', 
    },
    '20181001_30d':{
        'init_day': '20181001', 
        'end_day': '20181030', 
    },

    '20101001_30d':{
        'init_day': '20101001', 
        'end_day': '20101030', 
    },
    '20111001_30d':{
        'init_day': '20111001', 
        'end_day': '20111030', 
    },

    '20120401_30d':{
        'init_day': '20120401', 
        'end_day': '20120430', 
    },

    '20120601_30d':{
        'init_day': '20120601', 
        'end_day': '20120630', 
    },

    '20120101_30d':{
        'init_day': '20120101', 
        'end_day': '20120130', 
    },
    '20111201_30d':{
        'init_day': '20111201', 
        'end_day': '20111230', 
    },
    '20081001_30d':{
        'init_day': '20081001', 
        'end_day': '20081030', 
    },
    '20120801_30d':{
        'init_day': '20120801', 
        'end_day': '20120830', 
    },
    '20120701_30d':{
        'init_day': '20120701', 
        'end_day': '20120730', 
    },
    '20120901_30d':{
        'init_day': '20120901', 
        'end_day': '20120930', 
    },

}
