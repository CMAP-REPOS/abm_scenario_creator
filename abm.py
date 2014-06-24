#!/usr/bin/env python
'''
    abm.py
    Author: npeterson
    Revised: 6/24/14
    ---------------------------------------------------------------------------
    A script for reading ABM output files into a database and/or Python objects
    for querying and summarization.

'''
import os
import sys
import csv
import sqlite3


class ABM(object):
    ''' A class for loading ABM model run output data into a SQLite database.
        Initialized with path (parent directory of 'model') and model run
        sample rate (default 0.05). '''

    # Set model-agnostic variables
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

    def __init__(self, abm_dir, sample_rate=0.05):
        # Set model-specific properties
        self.dir = abm_dir
        self.sample_rate = sample_rate
        self.name = os.path.basename(self.dir)
        self._output_dir = os.path.join(self.dir, 'model', 'outputs')
        self._hh_data_csv = os.path.join(self._output_dir, 'hhData_1.csv')
        self._tours_indiv_csv = os.path.join(self._output_dir, 'indivTourData_1.csv')
        self._tours_joint_csv = os.path.join(self._output_dir, 'jointTourData_1.csv')
        self._trips_indiv_csv = os.path.join(self._output_dir, 'indivTripData_1.csv')
        self._trips_joint_csv = os.path.join(self._output_dir, 'jointTripData_1.csv')

        # Create DB
        print 'Creating database for {0}...'.format(self.name)
        self._con = sqlite3.connect(':memory:')
        self._con.row_factory = sqlite3.Row

        # Load data from CSVs
        # -- Households table
        print 'Loading households...'
        self._con.execute('''CREATE TABLE households (
            id INTEGER PRIMARY KEY,
            sz INTEGER,
            size INTEGER
        )''')

        with open(self._hh_data_csv, 'rb') as csvfile:
            r = csv.DictReader(csvfile)
            for d in r:
                hh_id = int(d['hh_id'])
                sz = int(d['maz'])
                size = int(d['size'])
                db_row = (hh_id, sz, size)
                self._con.execute('INSERT INTO households VALUES (?,?,?)', db_row)

        self.households = self._count_rows('households')
        print '-- Households: {0:>14,.0f}'.format(self.households)

        # -- Tours table
        print 'Loading tours...'
        self._con.execute('''CREATE TABLE tours (
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

        with open(self._tours_indiv_csv, 'rb') as csvfile:
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
                self._con.execute('INSERT INTO tours VALUES (?,?,?,?,?,?,?,?,?,?)', db_row)

        with open(self._tours_joint_csv, 'rb') as csvfile:
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
                self._con.execute('INSERT INTO tours VALUES (?,?,?,?,?,?,?,?,?,?)', db_row)

        self.tours_indiv = self._count_rows('tours', 'is_joint=0')
        self.tours_joint = self._count_rows('tours', 'is_joint=1')
        self.tours = self.tours_indiv + self.tours_joint
        print '-- Tours (indiv): {0:>11,.0f}'.format(self.tours_indiv)
        print '-- Tours (joint): {0:>11,.0f}'.format(self.tours_joint)
        print '-- Tours (total): {0:>11,.0f}'.format(self.tours)

        # -- Trips table
        print 'Loading trips...'
        self._con.execute('''CREATE TABLE trips (
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

        with open(self._trips_indiv_csv, 'rb') as csvfile:
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
                self._con.execute('INSERT INTO trips VALUES (?,?,?,?,?,?,?,?,?,?,?)', db_row)

        with open(self._trips_joint_csv, 'rb') as csvfile:
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
                self._con.execute('INSERT INTO trips VALUES (?,?,?,?,?,?,?,?,?,?,?)', db_row)

        self.trips_indiv = self._count_rows('trips', 'is_joint=0')
        self.trips_joint = self._count_rows('trips', 'is_joint=1')
        self.trips = self.trips_indiv + self.trips_joint
        print '-- Trips (indiv): {0:>11,.0f}'.format(self.trips_indiv)
        print '-- Trips (joint): {0:>11,.0f}'.format(self.trips_joint)
        print '-- Trips (total): {0:>11,.0f}'.format(self.trips)

        self._con.commit()
        return

    def __str__(self):
        return '[ABM: {0} ({1:.0%} sample)]'.format(self.name, self.sample_rate)


    def close_db(self):
        ''' Close the DB. '''
        self._con.close()

    def _count_rows(self, table, where_clause=None):
        ''' Execute a SELECT COUNT(*) query on a table with optional where
            clause and return the integer result. '''
        query = 'SELECT COUNT(*) FROM {0}'.format(table)
        if where_clause:
            query += ' WHERE {0}'.format(where_clause)
        return float(self._con.execute(query).fetchone()[0])

    @classmethod
    def _get_mode_str(cls, mode_num):
        ''' Return description of a mode code. '''
        return cls.modes[mode_num]

    def print_mode_share(self, table):
        ''' Display the mode share of trips or tours. '''
        table_rows = self._count_rows(table)
        mode_share = {}
        for mode in sorted(self.modes.keys()):
            mode_share[mode] = self._count_rows(table, 'mode={0}'.format(mode)) / table_rows
            print '{0:.<30}{1:>7.2%}'.format(self._get_mode_str(mode), mode_share[mode])
        return

    def query(self, sql_query):
        ''' Execute a SQL query and return the cursor object. '''
        return self._con.execute(sql_query)

    #def create_objects(self):
    #    ''' Convert SQL rows into Python objects. SUPER SLOW!!! '''
    #    households = []
    #    counter = {'HH': 0, 'TOUR': 0, 'TRIP': 0}
    #
    #    print 'Creating household objects...'
    #    for row in self._con.execute('SELECT * FROM households'):
    #        households.append(Household(*row))
    #        counter['HH'] += 1
    #        if counter['HH'] % 10000 == 0: print counter['HH']
    #
    #    print 'Creating tour objects...'
    #    for hh in households:
    #        for row in self._con.execute('SELECT * FROM tours WHERE hh_id=?', (hh.id,)):
    #            hh.add_tour(Tour(*row))
    #            counter['TOUR'] += 1
    #            if counter['TOUR'] % 10000 == 0: print counter['TOUR']
    #
    #    print 'Creating trip objects...'
    #    for hh in households:
    #        for tour in hh.tours:
    #            for row in self._con.execute('SELECT * FROM trips WHERE tour_id=?', (tour.id,)):
    #                tour.add_trip(Trip(*row))
    #                counter['TRIP'] += 1
    #                if counter['TRIP'] % 10000 == 0: print counter['TRIP']
    #
    #    return households


class Comparison(object):
    ''' A class for comparing two ABM objects. '''
    def __init__(self, base_abm, test_abm):
        self.base_abm = base_abm
        self.test_abm = test_abm
        return

    def __str__(self):
        return '[Comparison: BASE {0}; TEST {1}]'.format(self.base_abm, self.test_abm)


#class Household(object):
#    ''' A modeled household. '''
#    def __init__(self, hh_id, sz, size):
#        self.id = hh_id
#        self.sz = sz
#        self.size = size
#        self.tours = []
#        return
#
#    def __str__(self):
#        return '[Household: {0}]'.format(self.id)
#
#    def add_tour(self, tour):
#        ''' Add a tour to the household. '''
#        self.tours.append(tour)
#        return


#class Tour(object):
#    ''' A modeled tour. '''
#    def __init__(self, tour_id, hh_id, participants, pers_id, is_joint, category, purpose, sz_o, sz_d, mode):
#        self.id = tour_id
#        self.hh_id = hh_id
#        self.participants = [int(p) for p in participants.split()]
#        self.pers_id = pers_id
#        self.is_joint = is_joint
#        self._category = category
#        self.purpose = purpose
#        self.sz_o = sz_o
#        self.sz_d = sz_d
#        self.mode = mode
#        self.trips = []
#        return
#
#    def __str__(self):
#        return '[Tour: {0}]'.format(self.id)
#
#    def add_trip(self, trip):
#        ''' Add a trip to the tour. '''
#        self.trips.append(trip)
#        return


#class Trip(object):
#    ''' A modeled trip. '''
#    def __init__(self, trip_id, tour_id, hh_id, pers_id, is_joint, inbound, purpose_o, purpose_d, sz_o, sz_d, mode):
#        self.id = trip_id
#        self.tour_id = tour_id
#        self.hh_id = hh_id
#        self.pers_id = pers_id
#        self.is_joint = is_joint
#        self.inbound = inbound
#        self.purpose_o = purpose_o
#        self.purpose_d = purpose_d
#        self.sz_o = sz_o
#        self.sz_d = sz_d
#        self.mode = mode
#        return
#
#    def __str__(self):
#        return '[Trip: {0}]'.format(self.id)



### SCRIPT MODE ###
def main():
    base_dir = r'Y:\nmp\basic_template_20140521'
    test_dir = r'Y:\nmp\basic_template_20140521'
    base_abm = ABM(base_dir)
    print base_abm
    test_abm = ABM(test_dir)
    print test_abm
    comparison = Comparison(base_abm, test_abm)
    print comparison

if __name__ == '__main__':
    main()
