#!/usr/bin/env python
'''
    abm.py
    Author: npeterson
    Revised: 6/25/14
    ---------------------------------------------------------------------------
    A script for reading ABM output files into a database and/or Python objects
    for querying and summarization.

'''
import os
import sys
import csv
import sqlite3
from inro.emme.database import emmebank as _eb


class ABM(object):
    ''' A class for loading ABM model run output data into a SQLite database.
        Initialized with path (parent directory of 'model') and model run
        sample rate (default 0.05). '''

    # Instance initialization & methods
    def __init__(self, abm_dir, sample_rate=0.05):
        self.dir = abm_dir
        self.sample_rate = sample_rate
        self.name = os.path.basename(self.dir)

        # Open Emmebank and load highway matrices
        print 'Loading Emme matrices in memory...'
        self._emmebank_path = os.path.join(self.dir, 'model', 'CMAP-ABM', 'Database', 'emmebank')
        self._emmebank = _eb.Emmebank(self._emmebank_path)
        self._matrices = {}
        for mode in xrange(1, 7):
            self._matrices[mode] = {}
            t, d = self._get_matrix_nums(mode)
            for tod in xrange(1, 9):
                self._matrices[mode][tod] = {}
                self._matrices[mode][tod]['t'] = self._emmebank.matrix('mf{0}{1}'.format(tod, t)).get_data(tod)
                self._matrices[mode][tod]['d'] = self._emmebank.matrix('mf{0}{1}'.format(tod, d)).get_data(tod)
        self._emmebank.dispose()  # Close Emmebank, remove lock

        # Set CT-RAMP output paths
        self._output_dir = os.path.join(self.dir, 'model', 'outputs')
        self._hh_data_csv = os.path.join(self._output_dir, 'hhData_1.csv')
        self._tours_indiv_csv = os.path.join(self._output_dir, 'indivTourData_1.csv')
        self._tours_joint_csv = os.path.join(self._output_dir, 'jointTourData_1.csv')
        self._trips_indiv_csv = os.path.join(self._output_dir, 'indivTripData_1.csv')
        self._trips_joint_csv = os.path.join(self._output_dir, 'jointTripData_1.csv')

        # Create DB to store CT-RAMP output
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
        print '-- Households: {0:>12.0f}'.format(self.households)

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
            tod_d INTEGER,
            tod_a INTEGER,
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
                tod_d = self._convert_time_period(int(d['depart_period']))
                tod_a = self._convert_time_period(int(d['arrive_period']))
                mode = int(d['tour_mode'])
                tour_id = '{0}-{1}-{2}-{3}'.format(hh_id, pers_id, tour_num, purpose)
                is_joint = False

                db_row = (tour_id, hh_id, participants, pers_id, is_joint, category, purpose, sz_o, sz_d, tod_d, tod_a, mode)
                self._con.execute('INSERT INTO tours VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', db_row)

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
                tod_d = self._convert_time_period(int(d['depart_period']))
                tod_a = self._convert_time_period(int(d['arrive_period']))
                mode = int(d['tour_mode'])
                tour_id = '{0}-{1}-{2}-{3}'.format(hh_id, pers_id, tour_num, purpose)
                is_joint = True

                db_row = (tour_id, hh_id, participants, pers_id, is_joint, category, purpose, sz_o, sz_d, tod_d, tod_a, mode)
                self._con.execute('INSERT INTO tours VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', db_row)

        self.tours_indiv = self._count_rows('tours', 'is_joint=0')
        self.tours_joint = self._count_rows('tours', 'is_joint=1')
        self.tours = self.tours_indiv + self.tours_joint
        print '-- Tours (indiv): {0:>9.0f}'.format(self.tours_indiv)
        print '-- Tours (joint): {0:>9.0f}'.format(self.tours_joint)
        print '-- Tours (total): {0:>9.0f}'.format(self.tours)

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
            zn_o INTEGER,
            zn_d INTEGER,
            tap_o INTEGER,
            tap_d INTEGER,
            tod INTEGER,
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
                zn_o = int(d['orig_taz'])
                zn_d = int(d['dest_taz'])
                tap_o = int(d['board_tap'])
                tap_d = int(d['alight_tap'])
                tod = self._convert_time_period(int(d['stop_period']))
                mode = int(d['trip_mode'])
                tour_id = '{0}-{1}-{2}-{3}'.format(hh_id, pers_id, tour_num, purpose_t)
                trip_id = '{0}-{1}-{2}'.format(tour_id, inbound, stop_id)
                is_joint = False

                db_row = (trip_id, tour_id, hh_id, pers_id, is_joint, inbound, purpose_o, purpose_d, sz_o, sz_d, zn_o, zn_d, tap_o, tap_d, tod, mode)
                self._con.execute('INSERT INTO trips VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', db_row)

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
                zn_o = int(d['orig_taz'])
                zn_d = int(d['dest_taz'])
                tap_o = int(d['board_tap'])
                tap_d = int(d['alight_tap'])
                tod = self._convert_time_period(int(d['stop_period']))
                mode = int(d['trip_mode'])
                tour_id = '{0}-{1}-{2}-{3}'.format(hh_id, pers_id, tour_num, purpose_t)
                trip_id = '{0}-{1}-{2}'.format(tour_id, inbound, stop_id)
                is_joint = True

                db_row = (trip_id, tour_id, hh_id, pers_id, is_joint, inbound, purpose_o, purpose_d, sz_o, sz_d, zn_o, zn_d, tap_o, tap_d, tod, mode)
                self._con.execute('INSERT INTO trips VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', db_row)

        self.trips_indiv = self._count_rows('trips', 'is_joint=0')
        self.trips_joint = self._count_rows('trips', 'is_joint=1')
        self.trips = self.trips_indiv + self.trips_joint
        print '-- Trips (indiv): {0:>9.0f}'.format(self.trips_indiv)
        print '-- Trips (joint): {0:>9.0f}'.format(self.trips_joint)
        print '-- Trips (total): {0:>9.0f}'.format(self.trips)

        self._con.commit()

        del self._matrices

        return None  ### End of ABM.__init__() ###

    def __str__(self):
        return '[ABM: {0} ({1:.0%} sample)]'.format(self.name, self.sample_rate)

    # Properties
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

    tod_by_index = [
        None,                  # No CT-RAMP period 0
        1,1,1,                 # CT-RAMP periods 1-3: [3am, 6am)
        2,2,                   # CT-RAMP periods 4-5: [6am, 7am)
        3,3,3,3,               # CT-RAMP periods 6-9: [7am, 9am)
        4,4,                   # CT-RAMP periods 10-11: [9am, 10am)
        5,5,5,5,5,5,5,5,       # CT-RAMP periods 12-19: [10am, 2pm)
        6,6,6,6,               # CT-RAMP periods 20-23: [2pm, 4pm)
        7,7,7,7,               # CT-RAMP periods 24-27: [4pm, 6pm)
        8,8,8,8,               # CT-RAMP periods 28-31: [6pm, 8pm)
        1,1,1,1,1,1,1,1,1,1,1  # CT-RAMP periods 32-42: [8pm, 3am)
    ]

    # Class methods
    @classmethod
    def _convert_time_period(cls, in_period, ctramp_to_emme=True):
        ''' Convert CT-RAMP time period to Emme time-of-day, or vice versa.
            Uses a list with values corresponding to Emme TOD and index values
            corresponding to CT-RAMP periods: all 30-minute intervals, except
            some in TOD 1 (overnight). '''
        if ctramp_to_emme:
            return cls.tod_by_index[in_period]
        else:
            return [index for index, tod in enumerate(cls.tod_by_index) if tod == in_period]

    @classmethod
    def _get_matrix_nums(cls, mode):
        ''' Return the matrix numbers for congested time and distance
            corresponding to driving mode (1-6). '''
        if mode == 1:  # SOV, no toll
            t, d = 175, 177
        elif mode == 2:  # SOV, toll
            t, d = 180, 183
        elif mode == 3:  # HOV2, no toll
            t, d = 185, 187
        elif mode == 4:  # HOV2, toll
            t, d = 190, 193
        elif mode == 5:  # HOV3+, no toll
            t, d = 195, 197
        elif mode == 6:  # HOV3+, toll
            t, d = 200, 203
        else:
            t, d = None, None
        return (t, d)

    @classmethod
    def _get_mode_str(cls, mode_num):
        ''' Return description of a mode code. '''
        return cls.modes[mode_num]

    # Instance methods
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


class Comparison(object):
    ''' A class for comparing two ABM objects. '''
    def __init__(self, base_abm, test_abm):
        self.base_abm = base_abm
        self.test_abm = test_abm
        return None

    def __str__(self):
        return '[Comparison: BASE {0}; TEST {1}]'.format(self.base_abm, self.test_abm)



### SCRIPT MODE ###
def main():
    base_dir = r'Y:\nmp\basic_template_20140521'
    base_abm = ABM(base_dir)
    print base_abm

    test_dir = r'Y:\nmp\basic_template_20140527'
    test_abm = ABM(test_dir)
    print test_abm

    comparison = Comparison(base_abm, test_abm)
    print comparison

    return None

if __name__ == '__main__':
    main()
