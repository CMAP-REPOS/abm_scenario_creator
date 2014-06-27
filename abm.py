#!/usr/bin/env python
'''
    abm.py
    Author: npeterson
    Revised: 6/26/14
    ---------------------------------------------------------------------------
    A script for reading ABM output files and matrix data into an SQL database
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
    def __init__(self, abm_dir, sample_rate=0.05):
        self.dir = abm_dir
        self.sample_rate = sample_rate
        self.name = os.path.basename(self.dir)
        self._output_dir = os.path.join(self.dir, 'model', 'outputs')
        self._db = os.path.join(self._output_dir, '{0}.db'.format(self.name))
        if os.path.exists(self._db):
            print 'Removing existing database...'
            os.remove(self._db)

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
        self._hh_data_csv = os.path.join(self._output_dir, 'hhData_1.csv')
        self._tours_indiv_csv = os.path.join(self._output_dir, 'indivTourData_1.csv')
        self._tours_joint_csv = os.path.join(self._output_dir, 'jointTourData_1.csv')
        self._trips_indiv_csv = os.path.join(self._output_dir, 'indivTripData_1.csv')
        self._trips_joint_csv = os.path.join(self._output_dir, 'jointTripData_1.csv')

        # Create DB to store CT-RAMP output
        print 'Initializing database ({0})...'.format(self._db)
        self.open_db()

        # Load data from CSVs
        # -- Households table
        print 'Loading households...'
        self._con.execute('''CREATE TABLE Households (
            hh_id INTEGER PRIMARY KEY,
            sz INTEGER,
            size INTEGER
        )''')
        self._insert_households(self._hh_data_csv)
        self._con.commit()

        self.households = self._unsample(self._count_rows('Households'))
        print '-- Households: {0:>14.0f}'.format(self.households)

        # -- Tours table
        print 'Loading tours...'
        self._con.execute('''CREATE TABLE Tours (
            tour_id TEXT PRIMARY KEY,
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
            FOREIGN KEY (hh_id) REFERENCES Households(id)
        )''')
        self._insert_tours(self._tours_indiv_csv, is_joint=False)
        self._insert_tours(self._tours_joint_csv, is_joint=True)
        self._con.commit()

        self.tours_indiv = self._unsample(self._count_rows('Tours', 'is_joint=0'))
        self.tours_joint = self._unsample(self._count_rows('Tours', 'is_joint=1'))
        self.tours = self.tours_indiv + self.tours_joint
        print '-- Tours (indiv): {0:>11.0f}'.format(self.tours_indiv)
        print '-- Tours (joint): {0:>11.0f}'.format(self.tours_joint)
        print '-- Tours (total): {0:>11.0f}'.format(self.tours)

        # -- Trips table
        print 'Loading trips...'
        self._con.execute('''CREATE TABLE Trips (
            trip_id TEXT PRIMARY KEY,
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
            time REAL,
            distance REAL,
            FOREIGN KEY (tour_id) REFERENCES Tours(tour_id),
            FOREIGN KEY (hh_id) REFERENCES Households(hh_id)
        )''')
        self._insert_trips(self._trips_indiv_csv, is_joint=False)
        self._insert_trips(self._trips_joint_csv, is_joint=True)
        self._con.commit()

        self.trips_indiv = self._unsample(self._count_rows('Trips', 'is_joint=0'))
        self.trips_joint = self._unsample(self._count_rows('Trips', 'is_joint=1'))
        self.trips = self.trips_indiv + self.trips_joint
        print '-- Trips (indiv): {0:>11.0f}'.format(self.trips_indiv)
        print '-- Trips (joint): {0:>11.0f}'.format(self.trips_joint)
        print '-- Trips (total): {0:>11.0f}'.format(self.trips)

        #self.close_db()

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
    def _clean_str(self, string):
        ''' Clean a string for database entry. '''
        return string.lower().replace('-', '').replace(' ', '')

    def close_db(self):
        ''' Close the database connection. '''
        return self._con.close()

    def _count_rows(self, table, where_clause=None):
        ''' Execute a SELECT COUNT(*) query on a table with optional where
            clause and return the integer result. '''
        query = 'SELECT COUNT(*) FROM {0}'.format(table)
        if where_clause:
            query += ' WHERE {0}'.format(where_clause)
        return float(self._con.execute(query).fetchone()[0])

    def get_mode_share(self, table, print_results=False):
        ''' Display the mode share of trips or tours. '''
        table_rows = self._count_rows(table)
        mode_share = {}
        for mode in sorted(self.modes.keys()):
            mode_share[mode] = self._count_rows(table, 'mode={0}'.format(mode)) / table_rows
            if print_results:
                print '{0:.<30}{1:>7.2%}'.format(self._get_mode_str(mode), mode_share[mode])
        return mode_share

    def _insert_households(self, hh_csv):
        with open(hh_csv, 'rb') as csvfile:
            r = csv.DictReader(csvfile)
            for d in r:
                hh_id = int(d['hh_id'])
                sz = int(d['maz'])
                size = int(d['size'])
                db_row = (hh_id, sz, size)
                insert_sql = 'INSERT INTO Households VALUES ({0})'.format(','.join(['?'] * len(db_row)))
                self._con.execute(insert_sql, db_row)

    def _insert_tours(self, tours_csv, is_joint):
        with open(tours_csv, 'rb') as csvfile:
            r = csv.DictReader(csvfile)
            for d in r:
                hh_id = int(d['hh_id'])
                participants = str(d['tour_participants']) if is_joint else str(d['person_num'])
                pers_id = 'J' if is_joint else participants
                tour_num = int(d['tour_id'])
                purpose = self._clean_str(str(d['tour_purpose']))
                category = self._clean_str(str(d['tour_category']))
                sz_o = int(d['orig_maz'])
                sz_d = int(d['dest_maz'])
                tod_d = self._convert_time_period(int(d['depart_period']))
                tod_a = self._convert_time_period(int(d['arrive_period']))
                mode = int(d['tour_mode'])
                tour_id = '{0}-{1}-{2}-{3}'.format(hh_id, pers_id, tour_num, purpose)
                db_row = (
                    tour_id, hh_id, participants, pers_id, is_joint, category,
                    purpose, sz_o, sz_d, tod_d, tod_a, mode
                )
                insert_sql = 'INSERT INTO Tours VALUES ({0})'.format(','.join(['?'] * len(db_row)))
                self._con.execute(insert_sql, db_row)
        return None

    def _insert_trips(self, trips_csv, is_joint):
        with open(trips_csv, 'rb') as csvfile:
            r = csv.DictReader(csvfile)
            for d in r:
                hh_id = int(d['hh_id'])
                pers_id = 'J' if is_joint else str(d['person_num'])
                tour_num = int(d['tour_id'])
                purpose_t = self._clean_str(str(d['tour_purpose']))
                inbound = int(d['inbound'])
                stop_id = int(d['stop_id']) + 1  # to avoid all the -1's
                purpose_o = self._clean_str(str(d['orig_purpose']))
                purpose_d = self._clean_str(str(d['dest_purpose']))
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
                if mode <= 6:
                    time = self._matrices[mode][tod]['t'].get(zn_o, zn_d)
                    distance = self._matrices[mode][tod]['d'].get(zn_o, zn_d)
                else:
                    time = None
                    distance = None
                db_row = (
                    trip_id, tour_id, hh_id, pers_id, is_joint, inbound,
                    purpose_o, purpose_d, sz_o, sz_d, zn_o, zn_d, tap_o, tap_d,
                    tod, mode, time, distance
                )
                insert_sql = 'INSERT INTO Trips VALUES ({0})'.format(','.join(['?'] * len(db_row)))
                self._con.execute(insert_sql, db_row)
        return None

    def open_db(self):
        ''' Open the database connection. '''
        self._con = sqlite3.connect(self._db)
        self._con.row_factory = sqlite3.Row
        return None

    def query(self, sql_query):
        ''' Execute a SQL query and return the cursor object. '''
        return self._con.execute(sql_query)

    def _unsample(self, num, sample_rate=None):
        ''' Divide a number by sample rate to approximate 100% sample. '''
        if not sample_rate:
            sample_rate = self.sample_rate
        return num / sample_rate


class Comparison(object):
    ''' A class for comparing two ABM objects. '''
    def __init__(self, base_abm, test_abm):
        self.base = base_abm
        self.test = test_abm
        return None

    def __str__(self):
        return '[Comparison: BASE {0}; TEST {1}]'.format(self.base_abm, self.test_abm)

    # Instance methods
    def close_dbs(self):
        ''' Close base & test ABM database connections. '''
        self.base.close_db()
        self.test.close_db()
        return None

    def open_dbs(self):
        ''' Open base & test ABM database connections. '''
        self.base.open_db()
        self.test.open_db()
        return None

    def print_daily_auto_trips_diverted(self):
        base_drive_trans = self.base._unsample(self.base._count_rows('Trips', 'mode = 11 or mode = 12'))
        test_drive_trans = self.test._unsample(self.test._count_rows('Trips', 'mode = 11 or mode = 12'))
        div_auto_trips = test_drive_trans - base_drive_trans
        print 'Base daily drive-to-transit: {0:>11.0f}'.format(base_drive_trans)
        print 'Test daily drive-to-transit: {0:>11.0f}'.format(test_drive_trans)
        print 'Daily auto trips diverted: {0:>13.0f}'.format(div_auto_trips)
        return None

    def print_daily_auto_trips_eliminated(self):
        base_auto_trips = 0
        base_auto_trip_dist_sum = 0
        base_auto_trip_time_sum = 0
        base_auto_trip_dists = self.base.query('SELECT distance, time from Trips WHERE mode < 7')
        for trip in base_auto_trip_dists:
            base_auto_trips += self.base._unsample(1)
            base_auto_trip_dist_sum += self.base._unsample(trip[0])
            base_auto_trip_time_sum += self.base._unsample(trip[1])

        test_auto_trips = 0
        test_auto_trip_dist_sum = 0
        test_auto_trip_time_sum = 0
        test_auto_trip_dists = self.test.query('SELECT distance, time from Trips WHERE mode < 7')
        for trip in test_auto_trip_dists:
            test_auto_trips += self.test._unsample(1)
            test_auto_trip_dist_sum += self.test._unsample(trip[0])
            test_auto_trip_time_sum += self.test._unsample(trip[1])

        elim_auto_trips = base_auto_trips - test_auto_trips
        pct_elim = elim_auto_trips / base_auto_trips
        avg_dist_elim = (base_auto_trip_dist_sum - test_auto_trip_dist_sum) / elim_auto_trips
        avg_dur_elim = (base_auto_trip_time_sum - test_auto_trip_time_sum) / elim_auto_trips
        avg_speed_elim = avg_dist_elim / (avg_dur_elim / 60)

        print 'Base daily auto trips: {0:>17.0f}'.format(base_auto_trips)
        print 'Test daily auto trips: {0:>17.0f}'.format(test_auto_trips)
        print 'Daily auto trips eliminated: {0:>11.0f} ({1:.3%})'.format(elim_auto_trips, pct_elim)
        print 'Avg. distance of eliminated trip: {0:.2f} miles'.format(avg_dist_elim)
        print 'Avg. duration of eliminated trip: {0:.2f} mins'.format(avg_dur_elim)
        print 'Avg. speed of eliminated trip: {0:.2f} mph'.format(avg_speed_elim)
        return None

    def print_mode_share_change(self, grouped=False):
        base_mode_share = self.base.get_mode_share('Tours')
        test_mode_share = self.test.get_mode_share('Tours')
        mode_share_diff = {}
        for mode in sorted(base_mode_share.keys()):
            mode_share_diff[mode] = test_mode_share[mode] - base_mode_share[mode]
        if grouped:
            mode_share_grouped_diff = {
                'Drive (Excl. Taxi)': sum(mode_share_diff[m] for m in xrange(1, 7)),
                'Transit (Drive To)': sum(mode_share_diff[m] for m in xrange(11, 13)),
                'Transit (Walk To)': sum(mode_share_diff[m] for m in xrange(9, 11)),
                'Walk/Bike/Taxi/School Bus': sum(mode_share_diff[m] for m in (7, 8, 13, 14))
            }
            for mode in sorted(mode_share_grouped_diff.keys()):
                print '{0:.<30}{1:>+8.3%}'.format(mode, mode_share_grouped_diff[mode])
        else:
            for mode in sorted(mode_share_diff.keys()):
                print '{0:.<30}{1:>+8.3%}'.format(self.base._get_mode_str(mode), mode_share_diff[mode])
        return None



### SCRIPT MODE ###
def main():
    base = ABM(r'Y:\nmp\basic_template_20140521')
    print base

    test = ABM(r'Y:\nmp\basic_template_20140527')
    print test

    comparison = Comparison(base, test)
    print comparison

    comparison.print_daily_auto_trips_diverted()
    comparison.print_daily_auto_trips_eliminated()
    comparison.print_mode_share_change()

    return None

if __name__ == '__main__':
    main()
