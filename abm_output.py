#!/usr/bin/env python
'''
    abm_output.py
    Author: npeterson
    Revised: 6/19/14
    ---------------------------------------------------------------------------
    A script for reading ABM output files into Python objects.

'''
import os
import sys
import csv
import sqlite3


### GLOBAL METHODS ###
def get_mode_str(mode_num):
    modes = {
        1: 'Drive alone free',
        2: 'Drive alone pay',
        3: 'Shared ride 2 free',
        4: 'Shared ride 2 pay',
        5: 'Shared ride 3+ free',
        6: 'Shared ride 3+ pay',
        7: 'Walk',
        8: 'Bike',
        9: 'Walk to local transit',
        10: 'Walk to premium transit',
        11: 'Drive to local transit',
        12: 'Drive to premium transit',
        13: 'Taxi',
        14: 'School bus'
    }
    return modes[mode_num]



### ABM OUTPUT CLASSES ###
class ABM_DB(object):
    ''' A wrapper for a SQLite DB storing ABM output data. '''
    def __init__(self, abm_output_dir):
        # Set paths to CSVs
        self.hh_data_csv = os.path.join(abm_output_dir, 'hhData_1.csv')
        self.tours_indiv_csv = os.path.join(abm_output_dir, 'indivTourData_1.csv')
        self.tours_joint_csv = os.path.join(abm_output_dir, 'jointTourData_1.csv')
        self.trips_indiv_csv = os.path.join(abm_output_dir, 'indivTripData_1.csv')
        self.trips_joint_csv = os.path.join(abm_output_dir, 'jointTripData_1.csv')

        # Create DB
        self.conn = sqlite3.connect(':memory:')
        self.c = self.conn.cursor()
        self.c.execute('''CREATE TABLE households
                  (hh_id, maz, size)''')
        self.c.execute('''CREATE TABLE tours
                  (hh_id)''')
        self.c.execute('''CREATE TABLE trips
                  (hh_id)''')

        # Load data from CSVs


        self.conn.commit()
        return

    def close():
        ''' Close the DB. '''
        self.conn.close()



class Household(object):
    ''' A modeled household. '''
    def __init__(self, hh_id, maz, size):
        self.id = int(hh_id)
        self.sz = int(maz)
        self.size = int(size)
        self.tours = {}
        return

    def __str__(self):
        return '[Household: {0}]'.format(self.id)

    def add_tour(self, tour):
        ''' Add a tour to the household. '''
        self.tours[tour.id] = tour
        return


class Tour(object):
    ''' A modeled tour. '''
    def __init__(self, hh_id, tour_participants, tour_id, tour_category, tour_purpose, orig_maz, dest_maz, tour_mode):
        self.hh_id = int(hh_id)
        self.tour_id = int(tour_id)
        self.participants = [int(p) for p in str(tour_participants).split()]
        self.pers_id = str(self.participants[0]) if len(self.participants) == 1 else 'J'  # mark joint trips with 'J'
        self.is_joint = True if self.pers_id == 'J' else False
        self.category = str(tour_category)
        self.purpose = str(tour_purpose)
        self.sz_o = int(orig_maz)
        self.sz_d = int(dest_maz)
        self.mode = int(tour_mode)
        self.id = '{0}-{1}-{2}-{3}'.format(self.hh_id, self.pers_id, self.tour_id, self.purpose)
        self.trips = {}
        return

    def __str__(self):
        return '[Tour: {0}]'.format(self.id)

    def add_trip(self, trip):
        ''' Add a trip to the tour. '''
        self.trips[trip.id] = trip
        return


class Trip(object):
    ''' A modeled trip. '''
    def __init__(self, hh_id, person_num, tour_id, inbound, stop_id, tour_purpose, orig_purpose, dest_purpose, orig_maz, dest_maz, trip_mode):
        self.hh_id = int(hh_id)
        self.pers_id = str(person_num)
        self.is_joint = True if self.pers_id == 'J' else False
        self.tour_id = int(tour_id)
        self.stop_id = int(stop_id) + 1  # to avoid all the -1's
        self.inbound = int(inbound)
        self.purpose_t = str(tour_purpose)
        self.purpose_o = str(orig_purpose)
        self.purpose_d = str(dest_purpose)
        self.sz_o = int(orig_maz)
        self.sz_d = int(dest_maz)
        self.mode = int(trip_mode)
        self.id = '{0}-{1}-{2}-{3}-{4}-{5}'.format(self.hh_id, self.pers_id, self.tour_id, self.purpose_t, self.inbound, self.stop_id)
        return

    def __str__(self):
        return '[Trip: {0}]'.format(self.id)



### SCRIPT MODE ###
if __name__ == '__main__':

    abm_output_dir = r'Y:\nmp\basic_template_20140521\model\outputs'
    ABM = ABM_DB(abm_output_dir)
    #os.chdir(abm_output_dir)
    #hh_data_csv = 'hhData_1.csv'
    #tours_indiv_csv = 'indivTourData_1.csv'
    #tours_joint_csv = 'jointTourData_1.csv'
    #trips_indiv_csv = 'indivTripData_1.csv'
    #trips_joint_csv = 'jointTripData_1.csv'
    #
    ## Load trips
    #print 'Loading trips...'
    #trips = {}
    #
    #with open(trips_indiv_csv, 'rb') as csvfile:
    #    r = csv.DictReader(csvfile)
    #    for trip in r:
    #        trip_obj = Trip(trip['hh_id'], trip['person_num'], trip['tour_id'], trip['inbound'], trip['stop_id'], trip['tour_purpose'], trip['orig_purpose'], trip['dest_purpose'], trip['orig_maz'], trip['dest_maz'], trip['trip_mode'])
    #        trips[trip_obj.id] = trip_obj
    #print 'Individual trips: ', len(trips)  # 1,817,976 in base run CSV
    #
    #with open(trips_joint_csv, 'rb') as csvfile:
    #    r = csv.DictReader(csvfile)
    #    for trip in r:
    #        trip_obj = Trip(trip['hh_id'], 'J', trip['tour_id'], trip['inbound'], trip['stop_id'], trip['tour_purpose'], trip['orig_purpose'], trip['dest_purpose'], trip['orig_maz'], trip['dest_maz'], trip['trip_mode'])
    #        trips[trip_obj.id] = trip_obj
    #print 'Joint trips: ', len([1 for trip_id in trips if trips[trip_id].is_joint])  # 101,726 in base run CSV
    #print 'Total trips: ', len(trips)
    #print ' '
    #
    ## Load tours
    #print 'Loading tours...'
    #tours = {}
    #
    #with open(tours_indiv_csv, 'rb') as csvfile:
    #    r = csv.DictReader(csvfile)
    #    for tour in r:
    #        tour_obj = Tour(tour['hh_id'], tour['person_num'], tour['tour_id'], tour['tour_category'], tour['tour_purpose'], tour['orig_maz'], tour['dest_maz'], tour['tour_mode'])
    #        tours[tour_obj.id] = tour_obj
    #print '- Individual tours: ', len(trips)  # 720,369 in base run CSV
    #
    #with open(tours_joint_csv, 'rb') as csvfile:
    #    r = csv.DictReader(csvfile)
    #    for tour in r:
    #        tour_obj = Tour(tour['hh_id'], tour['tour_participants'], tour['tour_id'], tour['tour_category'], tour['tour_purpose'], tour['orig_maz'], tour['dest_maz'], tour['tour_mode'])
    #        tours[tour_obj.id] = tour_obj
    #print '- Joint tours: ', len([1 for tour_id in tours if tours[tour_id].is_joint])  # 50,863 in base run CSV
    #print '- Total tours: ', len(tours)
    #print ' '
    #
    ## Load households
    #print 'Loading households...'
    #households = {}
    #
    #with open(hh_data_csv, 'rb') as csvfile:
    #    r = csv.DictReader(csvfile)
    #    for hh in r:
    #        hh_obj = Household(hh['hh_id'], hh['maz'], hh['size'])
    #        households[hh_obj.id] = hh_obj
    #print '- Households: ', len(households)  # 193,316 in base run CSV
