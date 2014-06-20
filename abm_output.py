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
        print 'Creating database...'
        self.conn = sqlite3.connect(':memory:')
        self.c = self.conn.cursor()

        # Load data from CSVs
        # -- Households table
        print 'Loading households...'
        self.c.execute('''CREATE TABLE households (
            id INTEGER PRIMARY KEY,
            sz INTEGER,
            size INTEGER
        )''')

        with open(self.hh_data_csv, 'rb') as csvfile:
            r = csv.DictReader(csvfile)
            for d in r:
                hh_id = int(d['hh_id'])
                sz = int(d['maz'])
                size = int(d['size'])
                db_row = (hh_id, sz, size)
                self.c.execute('INSERT INTO households VALUES (?,?,?)', db_row)

        self.hh_count = sum((1 for r in self.c.execute('SELECT * FROM households')))
        print '-- Households: ', self.hh_count

        # -- Tours table
        print 'Loading tours...'
        self.c.execute('''CREATE TABLE tours (
            id TEXT PRIMARY KEY,
            hh_id INTEGER,
            participants TEXT,
            pers_id TEXT,
            is_joint BOOLEAN,
            category TEXT,
            purpose TEXT,
            sz_o INTEGER,
            sz_d INTEGER,
            mode INTEGER,
            FOREIGN KEY (hh_id) REFERENCES households(id)
        )''')

        with open(self.tours_indiv_csv, 'rb') as csvfile:
            r = csv.DictReader(csvfile)
            for d in r:
                hh_id = int(d['hh_id'])
                participants = str(d['person_num'])
                pers_id = participants
                tour_num = int(d['tour_id'])
                purpose = str(d['tour_purpose'])
                category = str(d['tour_category'])
                sz_o = int(d['orig_maz'])
                sz_d = int(d['dest_maz'])
                mode = int(d['tour_mode'])
                tour_id = '{0}-{1}-{2}-{3}'.format(hh_id, pers_id, tour_num, purpose)
                is_joint = False

                db_row = (tour_id, hh_id, participants, pers_id, is_joint, category, purpose, sz_o, sz_d, mode)
                self.c.execute('INSERT INTO tours VALUES (?,?,?,?,?,?,?,?,?,?)', db_row)

        with open(self.tours_joint_csv, 'rb') as csvfile:
            r = csv.DictReader(csvfile)
            for d in r:
                hh_id = int(d['hh_id'])
                participants = str(d['tour_participants'])
                pers_id = 'J'
                tour_num = int(d['tour_id'])
                purpose = str(d['tour_purpose'])
                category = str(d['tour_category'])
                sz_o = int(d['orig_maz'])
                sz_d = int(d['dest_maz'])
                mode = int(d['tour_mode'])
                tour_id = '{0}-{1}-{2}-{3}'.format(hh_id, pers_id, tour_num, purpose)
                is_joint = True

                db_row = (tour_id, hh_id, participants, pers_id, is_joint, category, purpose, sz_o, sz_d, mode)
                self.c.execute('INSERT INTO tours VALUES (?,?,?,?,?,?,?,?,?,?)', db_row)

        self.tour_i_count = sum((1 for r in self.c.execute('SELECT * FROM tours WHERE is_joint=0')))
        self.tour_j_count = sum((1 for r in self.c.execute('SELECT * FROM tours WHERE is_joint=1')))
        self.tour_count = self.tour_i_count + self.tour_j_count
        print '-- Tours (indiv): ', self.tour_i_count
        print '-- Tours (joint): ', self.tour_j_count
        print '-- Tours (total): ', self.tour_count

        # -- Trips table
        print 'Loading trips...'
        self.c.execute('''CREATE TABLE trips (
            id TEXT PRIMARY KEY,
            tour_id TEXT,
            hh_id INTEGER,
            pers_id TEXT,
            is_joint BOOLEAN,
            inbound BOOLEAN,
            purpose_o TEXT,
            purpose_d TEXT,
            sz_o INTEGER,
            sz_d INTEGER,
            mode INTEGER,
            FOREIGN KEY (tour_id) REFERENCES tours(id),
            FOREIGN KEY (hh_id) REFERENCES households(id)
        )''')

        with open(self.trips_indiv_csv, 'rb') as csvfile:
            r = csv.DictReader(csvfile)
            for d in r:
                hh_id = int(d['hh_id'])
                pers_id = str(d['person_num'])
                tour_num = int(d['tour_id'])
                purpose_t = str(d['tour_purpose'])
                inbound = int(d['inbound'])
                stop_id = int(d['stop_id']) + 1  # to avoid all the -1's
                purpose_o = str(d['orig_purpose'])
                purpose_d = str(d['dest_purpose'])
                sz_o = int(d['orig_maz'])
                sz_d = int(d['dest_maz'])
                mode = int(d['trip_mode'])
                tour_id = '{0}-{1}-{2}-{3}'.format(hh_id, pers_id, tour_num, purpose_t)
                trip_id = '{0}-{1}-{2}'.format(tour_id, inbound, stop_id)
                is_joint = False

                db_row = (trip_id, tour_id, hh_id, pers_id, is_joint, inbound, purpose_o, purpose_d, sz_o, sz_d, mode)
                self.c.execute('INSERT INTO trips VALUES (?,?,?,?,?,?,?,?,?,?,?)', db_row)

        with open(self.trips_joint_csv, 'rb') as csvfile:
            r = csv.DictReader(csvfile)
            for d in r:
                hh_id = int(d['hh_id'])
                pers_id = 'J'
                tour_num = int(d['tour_id'])
                purpose_t = str(d['tour_purpose'])
                inbound = int(d['inbound'])
                stop_id = int(d['stop_id']) + 1  # to avoid all the -1's
                purpose_o = str(d['orig_purpose'])
                purpose_d = str(d['dest_purpose'])
                sz_o = int(d['orig_maz'])
                sz_d = int(d['dest_maz'])
                mode = int(d['trip_mode'])
                tour_id = '{0}-{1}-{2}-{3}'.format(hh_id, pers_id, tour_num, purpose_t)
                trip_id = '{0}-{1}-{2}'.format(tour_id, inbound, stop_id)
                is_joint = True

                db_row = (trip_id, tour_id, hh_id, pers_id, is_joint, inbound, purpose_o, purpose_d, sz_o, sz_d, mode)
                self.c.execute('INSERT INTO trips VALUES (?,?,?,?,?,?,?,?,?,?,?)', db_row)

        self.trip_i_count = sum((1 for r in self.c.execute('SELECT * FROM trips WHERE is_joint=0')))
        self.trip_j_count = sum((1 for r in self.c.execute('SELECT * FROM trips WHERE is_joint=1')))
        self.trip_count = self.trip_i_count + self.trip_j_count
        print '-- Trips (indiv): ', self.trip_i_count
        print '-- Trips (joint): ', self.trip_j_count
        print '-- Trips (total): ', self.trip_count

        self.conn.commit()
        return

    def close(self):
        ''' Close the DB. '''
        self.conn.close()

    #def create_objects(self):
    #    ''' Convert SQL rows into Python objects. SUPER SLOW!!! '''
    #    households = []
    #    counter = {'HH': 0, 'TOUR': 0, 'TRIP': 0}
    #
    #    print 'Creating household objects...'
    #    self.c.execute('SELECT * FROM households')
    #    for row in self.c:
    #        households.append(Household(*row))
    #        counter['HH'] += 1
    #        if counter['HH'] % 10000 == 0: print counter['HH']
    #
    #    print 'Creating tour objects...'
    #    for hh in households:
    #        self.c.execute('SELECT * FROM tours WHERE hh_id=?', (hh.id,))
    #        for row in self.c:
    #            hh.add_tour(Tour(*row))
    #            counter['TOUR'] += 1
    #            if counter['TOUR'] % 10000 == 0: print counter['TOUR']
    #
    #    print 'Creating trip objects...'
    #    for hh in households:
    #        for tour in hh.tours:
    #            self.c.execute('SELECT * FROM trips WHERE tour_id=?', (tour.id,))
    #            for row in self.c:
    #                tour.add_trip(Trip(*row))
    #                counter['TRIP'] += 1
    #                if counter['TRIP'] % 10000 == 0: print counter['TRIP']
    #
    #    return households


class Household(object):
    ''' A modeled household. '''
    def __init__(self, hh_id, sz, size):
        self.id = hh_id
        self.sz = sz
        self.size = size
        self.tours = []
        return

    def __str__(self):
        return '[Household: {0}]'.format(self.id)

    def add_tour(self, tour):
        ''' Add a tour to the household. '''
        self.tours.append(tour)
        return


class Tour(object):
    ''' A modeled tour. '''
    def __init__(self, tour_id, hh_id, participants, pers_id, is_joint, category, purpose, sz_o, sz_d, mode):
        self.id = tour_id
        self.hh_id = hh_id
        self.participants = [int(p) for p in participants.split()]
        self.pers_id = pers_id
        self.is_joint = is_joint
        self.category = category
        self.purpose = purpose
        self.sz_o = sz_o
        self.sz_d = sz_d
        self.mode = mode
        self.trips = []
        return

    def __str__(self):
        return '[Tour: {0}]'.format(self.id)

    def add_trip(self, trip):
        ''' Add a trip to the tour. '''
        self.trips.append(trip)
        return


class Trip(object):
    ''' A modeled trip. '''
    def __init__(self, trip_id, tour_id, hh_id, pers_id, is_joint, inbound, purpose_o, purpose_d, sz_o, sz_d, mode):
        self.id = trip_id
        self.tour_id = tour_id
        self.hh_id = hh_id
        self.pers_id = pers_id
        self.is_joint = is_joint
        self.inbound = inbound
        self.purpose_o = purpose_o
        self.purpose_d = purpose_d
        self.sz_o = sz_o
        self.sz_d = sz_d
        self.mode = mode
        return

    def __str__(self):
        return '[Trip: {0}]'.format(self.id)



### SCRIPT MODE ###
if __name__ == '__main__':
    abm_output_dir = r'Y:\nmp\basic_template_20140521\model\outputs'
    ABM = ABM_DB(abm_output_dir)
    #households = ABM.create_objects()
    ABM.close()
