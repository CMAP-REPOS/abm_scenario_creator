#!/usr/bin/env python
'''
    results.py
    Author: npeterson
    Revised: 8/6/14
    ---------------------------------------------------------------------------
    A module for reading TMM output files and matrix data into an SQL database
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
    def __init__(self, abm_dir, sample_rate=0.05, build_db=True):
        self.dir = abm_dir
        self.sample_rate = sample_rate
        self.name = os.path.basename(self.dir)
        self._input_dir = os.path.join(self.dir, 'model', 'inputs')
        self._output_dir = os.path.join(self.dir, 'model', 'outputs')
        self._TEST_DIR = r'C:\WorkSpace\Temp\ABM'                               ########## REMOVE LATER ##########
        self._db = os.path.join(self._TEST_DIR, '{0}.db'.format(self.name))     ########## CHANGE LATER ##########
        if build_db and os.path.exists(self._db):
            print 'Removing existing database...'
            os.remove(self._db)
        if not build_db and not os.path.exists(self._db):
            raise ValueError('SQLite database does not yet exist. Please set build_db=True.')

        # Set CT-RAMP CSV paths
        self._tap_attr_csv = os.path.join(self._input_dir, 'tap_attributes.csv')
        self._hh_data_csv = os.path.join(self._output_dir, 'hhData_1.csv')
        self._pers_data_csv = os.path.join(self._output_dir, 'personData_1.csv')
        self._tours_indiv_csv = os.path.join(self._output_dir, 'indivTourData_1.csv')
        self._tours_joint_csv = os.path.join(self._output_dir, 'jointTourData_1.csv')
        self._trips_indiv_csv = os.path.join(self._output_dir, 'indivTripData_1.csv')
        self._trips_joint_csv = os.path.join(self._output_dir, 'jointTripData_1.csv')

        # Open Emmebank and load highway matrices
        if build_db:
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

        # Load TAP data
        print 'Loading TAP data into memory...'
        self.tap_zones = {}
        with open(self._tap_attr_csv, 'rb') as csvfile:
            r = csv.DictReader(csvfile)
            for d in r:
                tap = int(d['tap_id'])
                zone = int(d['taz09'])
                self.tap_zones[tap] = zone

        # Create DB to store CT-RAMP output
        print 'Opening database ({0})...'.format(self._db)
        self.open_db()

        # Load data from CSVs
        # -- Households table
        print 'Processing households...'
        if build_db:
            self._con.execute('''CREATE TABLE Households (
                hh_id INTEGER PRIMARY KEY,
                sz INTEGER,
                size INTEGER
            )''')
            self._insert_households(self._hh_data_csv)
            self._con.commit()

        self.households = self._unsample(self._count_rows('Households'))
        print '{0:<20}{1:>10,.0f}'.format('-- Households:', self.households)

        # -- People table
        print 'Processing people...'
        if build_db:
            self._con.execute('''CREATE TABLE People (
                pers_id TEXT PRIMARY KEY,
                hh_id INTEGER,
                pers_num INTEGER,
                age INTEGER,
                gender TEXT,
                class_w_wtt INTEGER,
                class_w_pnr INTEGER,
                class_w_knr INTEGER,
                class_o_wtt INTEGER,
                class_o_pnr INTEGER,
                class_o_knr INTEGER,
                FOREIGN KEY (hh_id) REFERENCES Households(hh_id)
            )''')
            self._insert_people(self._pers_data_csv)
            self._con.commit()

        self.people = self._unsample(self._count_rows('People'))
        print '{0:<20}{1:>10,.0f}'.format('-- People:', self.people)

        # -- Tours table
        print 'Processing tours...'
        if build_db:
            self._con.execute('''CREATE TABLE Tours (
                tour_id TEXT PRIMARY KEY,
                hh_id INTEGER,
                participants TEXT,
                pers_num TEXT,
                is_joint BOOLEAN,
                category TEXT,
                purpose TEXT,
                sz_o INTEGER,
                sz_d INTEGER,
                tod_d INTEGER,
                tod_a INTEGER,
                mode INTEGER,
                FOREIGN KEY (hh_id) REFERENCES Households(hh_id)
            )''')
            self._con.execute('''CREATE TABLE PersonTours (
                ptour_id TEXT PRIMARY KEY,
                tour_id TEXT,
                hh_id INTEGER,
                pers_id TEXT,
                mode INTEGER,
                FOREIGN KEY (pers_id) REFERENCES People(pers_id),
                FOREIGN KEY (tour_id) REFERENCES Tours(tour_id),
                FOREIGN KEY (hh_id) REFERENCES Households(hh_id)
            )''')
            self._insert_tours(self._tours_indiv_csv, is_joint=False)
            self._insert_tours(self._tours_joint_csv, is_joint=True)
            self._con.commit()

        self.tours_indiv = self._unsample(self._count_rows('Tours', 'is_joint=0'))
        self.tours_joint = self._unsample(self._count_rows('Tours', 'is_joint=1'))
        self.tours = self.tours_indiv + self.tours_joint
        self.person_tours = self._unsample(self._count_rows('PersonTours'))
        print '{0:<20}{1:>10,.0f}'.format('-- Tours (indiv):', self.tours_indiv)
        print '{0:<20}{1:>10,.0f}'.format('-- Tours (joint):', self.tours_joint)
        print '{0:<20}{1:>10,.0f}'.format('-- Tours (total):', self.tours)
        print '{0:<20}{1:>10,.0f}'.format('-- Person-Tours:', self.person_tours)

        # -- Trips table
        print 'Processing trips...'
        if build_db:
            self._con.execute('''CREATE TABLE Trips (
                trip_id TEXT PRIMARY KEY,
                tour_id TEXT,
                hh_id INTEGER,
                pers_num TEXT,
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
                drive_time REAL,
                drive_distance REAL,
                drive_speed REAL,
                FOREIGN KEY (tour_id) REFERENCES Tours(tour_id),
                FOREIGN KEY (hh_id) REFERENCES Households(hh_id)
            )''')
            self._con.execute('''CREATE TABLE PersonTrips (
                ptrip_id TEXT PRIMARY KEY,
                ptour_id TEXT,
                trip_id TEXT,
                tour_id TEXT,
                hh_id INTEGER,
                pers_id TEXT,
                mode INTEGER,
                FOREIGN KEY (pers_id) REFERENCES People(pers_id),
                FOREIGN KEY (trip_id) REFERENCES Trips(trip_id),
                FOREIGN KEY (tour_id) REFERENCES Tours(tour_id),
                FOREIGN KEY (ptour_id) REFERENCES PersonTours(ptour_id),
                FOREIGN KEY (hh_id) REFERENCES Households(hh_id)
            )''')
            self._insert_trips(self._trips_indiv_csv, is_joint=False)
            self._insert_trips(self._trips_joint_csv, is_joint=True)
            self._con.commit()

        self.trips_indiv = self._unsample(self._count_rows('Trips', 'is_joint=0'))
        self.trips_joint = self._unsample(self._count_rows('Trips', 'is_joint=1'))
        self.trips = self.trips_indiv + self.trips_joint
        self.person_trips = self._unsample(self._count_rows('PersonTrips'))
        print '{0:<20}{1:>10,.0f}'.format('-- Trips (indiv):', self.trips_indiv)
        print '{0:<20}{1:>10,.0f}'.format('-- Trips (joint):', self.trips_joint)
        print '{0:<20}{1:>10,.0f}'.format('-- Trips (total):', self.trips)
        print '{0:<20}{1:>10,.0f}'.format('-- Person-Trips:', self.person_trips)

        if build_db:
            del self._matrices

        # -- TransitSegs table
        print 'Processing transit segments...'
        if build_db:
            self._con.execute('''CREATE TABLE TransitSegs (
                tseg_id TEXT PRIMARY KEY,
                tline_id TEXT,
                tseg_num INTEGER,
                inode INTEGER,
                jnode INTEGER,
                tod INTEGER,
                transit_mode TEXT,
                boardings REAL,
                passengers REAL,
                pass_hrs REAL,
                pass_mi REAL
            )''')
            self._insert_tsegs()
            self._con.commit()

        self.transit_segments = self._count_rows('TransitSegs')
        print '{0:<20}{1:>10,.0f}'.format('-- Transit Segments:', self.transit_segments)

        self.transit_stats = self._get_transit_stats()
        self.mode_share = self._get_mode_share()
        self.ptrips_by_class = self._get_ptrips_by_class()

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

    transit_modes = {
        'M': 'Metra Rail',
        'C': 'CTA Rail',
        'B': 'CTA Bus (Regular)',
        'E': 'CTA Bus (Express)',
        'L': 'Pace Bus (Local)',
        'P': 'Pace Bus (Regular)',
        'Q': 'Pace Bus (Express)'
    }

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
            corresponding to driving mode (1-6). See ABM User Guide p.36. '''
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

    def _get_mode_share(self, table='Trips'):
        ''' Return the mode share of trips (or tours). '''
        table_rows = self._count_rows(table)
        mode_share = {}
        for mode in sorted(self.modes.keys()):
            mode_share[mode] = self._count_rows(table, 'mode={0}'.format(mode)) / table_rows
        return mode_share

    def _get_ptrips_by_class(self, stratify_by_field=None):
        ''' Return count of transit person-trips, split by user class (1-3). '''
        ptrips_dict_template = {1: 0.0, 2: 0.0, 3: 0.0}
        sql_select = 'SELECT PersonTrips.mode, Tours.category, People.class_w_wtt, People.class_w_pnr, People.class_w_knr, People.class_o_wtt, People.class_o_pnr, People.class_o_knr'
        sql_from = 'FROM PersonTrips LEFT JOIN Tours ON PersonTrips.tour_id=Tours.tour_id LEFT JOIN People ON PersonTrips.pers_id=People.pers_id'
        sql_where = 'WHERE PersonTrips.mode IN (9, 10, 11, 12)'

        if stratify_by_field:
            sql_select += ', {0}'.format(stratify_by_field)
            if stratify_by_field.lower().startswith('trips.'):
                sql_from += ' LEFT JOIN Trips ON PersonTrips.trip_id=Trips.trip_id'
            elif stratify_by_field.lower().startswith('households.'):
                sql_from += ' LEFT JOIN Households ON PersonTrips.hh_id=Households.hh_id'
            elif stratify_by_field.lower().startswith('persontours.'):
                sql_from += ' LEFT JOIN PersonTours ON PersonTrips.ptour_id=PersonTours.ptour_id'

            table, field = stratify_by_field.split('.')
            sql_groups = 'SELECT DISTINCT {0} FROM {1}'.format(field, table)
            groups = [r[0] for r in self.query(sql_groups)]
        else:
            groups = [None]

        sql = ' '.join((sql_select, sql_from, sql_where))

        ptrips_by_class = {}
        for group in groups:
            ptrips_by_class[group] = ptrips_dict_template.copy()

        for r in self.query(sql):
            mode = r[0]
            category = r[1]

            if category == 'mandatory':  # Use "work" user classes
                wtt = r[2]
                pnr = r[3]
                knr = r[4]
            else:  # Use "non-work" user classes
                wtt = r[5]
                pnr = r[6]
                knr = r[7]

            if mode in (9, 10):
                uclass = wtt
            else:
                uclass = max(pnr, knr)

            if stratify_by_field:
                group = r[8]
            else:
                group = None

            ptrips_by_class[group][uclass] += self._unsample(1.0)

        if stratify_by_field:
            return ptrips_by_class
        else:
            return ptrips_by_class[None]

    def _get_transit_stats(self):
        ''' Return the boardings, passenger miles traveled and passenger hours
            traveled, by mode. '''
        transit_stats = {
            'BOARDINGS': {},
            'PMT': {},
            'PHT': {}
        }
        query = 'SELECT transit_mode, SUM(boardings), SUM(pass_mi), SUM(pass_hrs) FROM TransitSegs GROUP BY transit_mode'
        for r in self._con.execute(query):
            transit_mode, boardings, pass_mi, pass_hrs = r
            transit_stats['BOARDINGS'][transit_mode] = boardings
            transit_stats['PMT'][transit_mode] = pass_mi
            transit_stats['PHT'][transit_mode] = pass_hrs
        return transit_stats

    def _insert_households(self, hh_csv):
        ''' Populate the Households table from a CSV. '''
        with open(hh_csv, 'rb') as csvfile:
            r = csv.DictReader(csvfile)
            for d in r:
                hh_id = int(d['hh_id'])
                sz = int(d['maz'])
                size = int(d['size'])
                db_row = (hh_id, sz, size)
                insert_sql = 'INSERT INTO Households VALUES ({0})'.format(','.join(['?'] * len(db_row)))
                self._con.execute(insert_sql, db_row)
        return None

    def _insert_people(self, pers_csv):
        ''' Populate the People table from a CSV. '''
        with open(pers_csv, 'rb') as csvfile:
            r = csv.DictReader(csvfile)
            for d in r:
                hh_id = int(d['hh_id'])
                pers_num = int(d['person_num'])
                pers_id = '{0}-{1}'.format(hh_id, pers_num)  # NOTE: NOT the 'person_id' value from CSV
                age = int(d['age'])
                gender = self._clean_str(d['gender'])
                uc_w_w = int(d['user_class_work_walk'])
                uc_w_p = int(d['user_class_work_pnr'])
                uc_w_k = int(d['user_class_work_knr'])
                uc_o_w = int(d['user_class_non_work_walk'])
                uc_o_p = int(d['user_class_non_work_pnr'])
                uc_o_k = int(d['user_class_non_work_knr'])
                db_row = (pers_id, hh_id, pers_num, age, gender, uc_w_w, uc_w_p, uc_w_k, uc_o_w, uc_o_p, uc_o_k)
                insert_sql = 'INSERT INTO People VALUES ({0})'.format(','.join(['?'] * len(db_row)))
                self._con.execute(insert_sql, db_row)
        return None

    def _insert_tours(self, tours_csv, is_joint):
        ''' Populate the Tours and PersonTours tables from a CSV. '''
        with open(tours_csv, 'rb') as csvfile:
            r = csv.DictReader(csvfile)
            for d in r:
                hh_id = int(d['hh_id'])
                participants = str(d['tour_participants']) if is_joint else str(d['person_num'])
                pers_num = 'J' if is_joint else participants
                tour_num = int(d['tour_id'])
                purpose = self._clean_str(str(d['tour_purpose']))
                category = self._clean_str(str(d['tour_category']))
                sz_o = int(d['orig_maz'])
                sz_d = int(d['dest_maz'])
                tod_d = self._convert_time_period(int(d['depart_period']))
                tod_a = self._convert_time_period(int(d['arrive_period']))
                mode = int(d['tour_mode'])
                tour_id = '{0}-{1}-{2}-{3}'.format(hh_id, pers_num, tour_num, purpose)
                db_row = (
                    tour_id, hh_id, participants, pers_num, is_joint, category,
                    purpose, sz_o, sz_d, tod_d, tod_a, mode
                )
                insert_sql = 'INSERT INTO Tours VALUES ({0})'.format(','.join(['?'] * len(db_row)))
                self._con.execute(insert_sql, db_row)

                for participant in participants.strip().split():
                    pers_id = '{0}-{1}'.format(hh_id, participant)
                    ptour_id = '{0}-{1}'.format(tour_id, participant)
                    db_row = (
                        ptour_id, tour_id, hh_id, pers_id, mode
                    )
                    insert_sql = 'INSERT INTO PersonTours VALUES ({0})'.format(','.join(['?'] * len(db_row)))
                    self._con.execute(insert_sql, db_row)

        return None

    def _insert_trips(self, trips_csv, is_joint):
        ''' Populate the Trips and PersonTrips tables from a CSV. '''
        with open(trips_csv, 'rb') as csvfile:
            r = csv.DictReader(csvfile)
            for d in r:
                hh_id = int(d['hh_id'])
                pers_num = 'J' if is_joint else str(d['person_num'])
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
                tour_id = '{0}-{1}-{2}-{3}'.format(hh_id, pers_num, tour_num, purpose_t)
                trip_id = '{0}-{1}-{2}'.format(tour_id, inbound, stop_id)

                # Estimate DRIVE time, distance, speed
                if mode <= 6 :  # Private autos
                    time = self._matrices[mode][tod]['t'].get(zn_o, zn_d)
                    distance = self._matrices[mode][tod]['d'].get(zn_o, zn_d)
                elif mode >= 13 :  # Taxis, school buses (assume drive alone, free)
                    time = self._matrices[1][tod]['t'].get(zn_o, zn_d)
                    distance = self._matrices[1][tod]['d'].get(zn_o, zn_d)
                elif mode in (11, 12):  # Drive-to-transit (assume drive alone, free)
                    from_zone = self.tap_zones[tap_d] if inbound else zn_o
                    to_zone = zn_d if inbound else self.tap_zones[tap_o]
                    time = self._matrices[1][tod]['t'].get(from_zone, to_zone)
                    distance = self._matrices[1][tod]['d'].get(from_zone, to_zone)
                else:  # Walk, bike, walk-to-transit
                    time = 0
                    distance = 0
                speed = distance / (time / 60) if (time and distance) else 0

                db_row = (
                    trip_id, tour_id, hh_id, pers_num, is_joint, inbound,
                    purpose_o, purpose_d, sz_o, sz_d, zn_o, zn_d, tap_o, tap_d,
                    tod, mode, time, distance, speed
                )
                insert_sql = 'INSERT INTO Trips VALUES ({0})'.format(','.join(['?'] * len(db_row)))
                self._con.execute(insert_sql, db_row)

                tour_participants = [r[0] for r in self.query("SELECT participants FROM Tours WHERE tour_id = '{0}'".format(tour_id))][0]
                for participant in tour_participants.strip().split():
                    pers_id = '{0}-{1}'.format(hh_id, participant)
                    ptour_id = '{0}-{1}'.format(tour_id, participant)
                    ptrip_id = '{0}-{1}'.format(trip_id, participant)
                    db_row = (
                        ptrip_id, ptour_id, trip_id, tour_id, hh_id, pers_id, mode
                    )
                    insert_sql = 'INSERT INTO PersonTrips VALUES ({0})'.format(','.join(['?'] * len(db_row)))
                    self._con.execute(insert_sql, db_row)

        return None

    def _insert_tsegs(self):
        ''' Populate the TransitSegs table from Emme transit assignments. '''
        self._emmebank = _eb.Emmebank(self._emmebank_path)
        for tod in xrange(1, 9):
            scenario_id = '10{0}'.format(tod)
            scenario = self._emmebank.scenario(scenario_id)
            network = scenario.get_network()
            for tseg in network.transit_segments():
                inode = tseg.i_node
                jnode = tseg.j_node
                if inode and jnode:
                    link = tseg.link
                    tline = tseg.line
                    boardings = tseg.transit_boardings
                    passengers = tseg.transit_volume
                    pass_hrs = passengers * tseg.transit_time / 60.0
                    pass_mi = passengers * link.length
                    db_row = (
                        tseg.id, tline.id, tseg.number, inode.number, jnode.number,
                        tod, tline.mode.id, boardings, passengers, pass_hrs, pass_mi
                    )
                    insert_sql = 'INSERT INTO TransitSegs VALUES ({0})'.format(','.join(['?'] * len(db_row)))
                    self._con.execute(insert_sql, db_row)
        self._emmebank.dispose()  # Close Emmebank, remove lock
        return None

    def open_db(self):
        ''' Open the database connection. '''
        self._con = sqlite3.connect(self._db)
        self._con.row_factory = sqlite3.Row
        return None

    def print_mode_share(self, grouped=True):
        ''' Print the mode share of trips. '''
        print ' '
        if grouped:
            mode_share_grouped = {
                'Auto (Excl. Taxi)': sum(self.mode_share[m] for m in xrange(1, 7)),
                'Drive-to-Transit': sum(self.mode_share[m] for m in xrange(11, 13)),
                'Walk-to-Transit': sum(self.mode_share[m] for m in xrange(9, 11)),
                'Walk/Bike/Taxi/School Bus': sum(self.mode_share[m] for m in (7, 8, 13, 14))
            }
            print 'MODE SHARE (GROUPED)'
            print '--------------------'
            for mode in sorted(mode_share_grouped.keys()):
                print '{0:<25}{1:>10.2%}'.format(mode, mode_share_grouped[mode])
        else:
            print 'MODE SHARE'
            print '----------'
            for mode in sorted(self.modes.keys()):
                print '{0:<25}{1:>10.2%}'.format(self._get_mode_str(mode), self.mode_share[mode])
        print ' '
        return None

    def print_ptrips_by_class(self):
        ''' Print the number and percentage of transit person-trips, stratified
            by user class. '''
        print ' '
        print 'TRANSIT PERSON-TRIPS BY USER CLASS'
        print '----------------------------------'
        total_ptrips = sum(self.ptrips_by_class.itervalues())
        for uclass in sorted(self.ptrips_by_class.keys()):
            ptrips = self.ptrips_by_class[uclass]
            ptrips_pct = ptrips / total_ptrips
            print '{0:<25}{1:>10,.0f} ({2:.2%})'.format('User Class {0}'.format(uclass), ptrips, ptrips_pct)
        print '{0:<25}{1:>10,.0f}'.format('All User Classes', total_ptrips)
        print ' '
        return None

    def print_transit_stats(self, grouped=True):
        ''' Print the boardings, passenger miles traveled and passenger hours
            traveled, by mode or grouped. '''
        print ' '
        if grouped:
            total_boardings = sum(self.transit_stats['BOARDINGS'].itervalues())
            total_pmt = sum(self.transit_stats['PMT'].itervalues())
            total_pht = sum(self.transit_stats['PHT'].itervalues())
            print 'TRANSIT STATS'
            print '-------------'
            print ' {0:<15} | {1:<15} | {2:<15} '.format('Boardings', 'Pass. Miles', 'Pass. Hours')
            print '{0:-<17}|{0:-<17}|{0:-<17}'.format('')
            print ' {0:>15,.0f} | {1:>15,.0f} | {2:>15,.0f} '.format(total_boardings, total_pmt, total_pht)
        else:
            print 'TRANSIT STATS BY MODE'
            print '---------------------'
            print ' {0:<20} | {1:<15} | {2:<15} | {3:<15} '.format('Mode', 'Boardings', 'Pass. Miles', 'Pass. Hours')
            print '{0:-<22}|{0:-<17}|{0:-<17}|{0:-<17}'.format('')
            for mode_code, mode_desc in sorted(self.transit_modes.iteritems(), key=lambda (k, v): v):
                boardings = self.transit_stats['BOARDINGS'][mode_code]
                pmt = self.transit_stats['PMT'][mode_code]
                pht = self.transit_stats['PHT'][mode_code]
                print ' {0:<20} | {1:>15,.0f} | {2:>15,.0f} | {3:>15,.0f} '.format(mode_desc, boardings, pmt, pht)
        print ' '
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
        return '[Comparison: BASE {0}; TEST {1}]'.format(self.base, self.test)

    # Instance methods
    def close_dbs(self):
        ''' Close base & test ABM database connections. '''
        self.base.close_db()
        self.test.close_db()
        return None

    def export_persontrips_csv(self, csv_path, geography='zone', trip_end='origin'):
        ''' Export a CSV file containing the mean base & test user classes for
            person-trips originating/ending in each zone/subzone. '''
        if trip_end not in ('origin', 'destination'):
            print 'CSV not exported: trip_end must be "origin" or "destination"!'
            return None
        elif geography == 'zone':
            group_field = 'Trips.zn_{0}'.format(trip_end[0])
        elif geography == 'subzone':
            group_field = 'Trips.sz_{0}'.format(trip_end[0])
        else:
            print 'CSV not exported: geography must be "zone" or "subzone"!'
            return None

        max_id = {
            'zone': 1944,
            'subzone': 16819
        }

        grouped_base_ptrips_by_class = self.base._get_ptrips_by_class(group_field)
        grouped_test_ptrips_by_class = self.test._get_ptrips_by_class(group_field)

        with open(csv_path, 'wb') as w:
            w.write('{0}_{1},ptrips_base,mean_uclass_base,ptrips_test,mean_uclass_test,ptrips_diff,mean_uclass_diff\n'.format(geography, trip_end[0]))
            for geog_id in xrange(1, max_id[geography]+1):
                where_filter = '{0} = {1}'.format(group_field, geog_id)

                if geog_id in grouped_base_ptrips_by_class:
                    base_ptrips_by_class = grouped_base_ptrips_by_class[geog_id]
                    base_ptrips_total = sum(base_ptrips_by_class.itervalues())
                    if base_ptrips_total > 0:
                        mean_uclass_base = sum((uc * n for uc, n in base_ptrips_by_class.iteritems())) / base_ptrips_total
                    else:
                        mean_uclass_base = 0
                else:
                    mean_uclass_base = 0

                if geog_id in grouped_test_ptrips_by_class:
                    test_ptrips_by_class = grouped_test_ptrips_by_class[geog_id]
                    test_ptrips_total = sum(test_ptrips_by_class.itervalues())
                    if test_ptrips_total > 0:
                        mean_uclass_test = sum((uc * n for uc, n in test_ptrips_by_class.iteritems())) / test_ptrips_total
                    else:
                        mean_uclass_test = 0
                else:
                    mean_uclass_test = 0

                ptrips_total_diff = test_ptrips_total - base_ptrips_total
                mean_uclass_diff = mean_uclass_test - mean_uclass_base

                row_template = '{0},{1:.0f},{2:.4f},{3:.0f},{4:.4f},{5:.0f},{6:.4f}\n'
                w.write(row_template.format(geog_id, base_ptrips_total, mean_uclass_base, test_ptrips_total, mean_uclass_test, ptrips_total_diff, mean_uclass_diff))

        print 'Person-trips and mean user class by {0} {1} have been exported to {2}.\n'.format(trip_end, geography, csv_path)
        return None

    def open_dbs(self):
        ''' Open base & test ABM database connections. '''
        self.base.open_db()
        self.test.open_db()
        return None

    def print_mode_share_change(self, grouped=True):
        ''' Print the change in mode share, grouped into broader categories
            (default), or ungrouped. '''
        mode_share_diff = {}
        for mode in sorted(ABM.modes.keys()):
            mode_share_diff[mode] = self.test.mode_share[mode] - self.base.mode_share[mode]
        print ' '
        if grouped:
            mode_share_grouped_diff = {
                'Auto (Excl. Taxi)': sum(mode_share_diff[m] for m in xrange(1, 7)),
                'Drive-to-Transit': sum(mode_share_diff[m] for m in xrange(11, 13)),
                'Walk-to-Transit': sum(mode_share_diff[m] for m in xrange(9, 11)),
                'Walk/Bike/Taxi/School Bus': sum(mode_share_diff[m] for m in (7, 8, 13, 14))
            }
            print 'MODE SHARE CHANGE (GROUPED)'
            print '---------------------------'
            for mode in sorted(mode_share_grouped_diff.keys()):
                print '{0:<25}{1:>+10.2%}'.format(mode, mode_share_grouped_diff[mode])
        else:
            print 'MODE SHARE CHANGE'
            print '-----------------'
            for mode in sorted(mode_share_diff.keys()):
                print '{0:<25}{1:>+10.2%}'.format(self.base._get_mode_str(mode), mode_share_diff[mode])
        print ' '
        return None

    def print_new_for_mode(self, mode_list, mode_description, table='Trips'):
        ''' Identify the increase (or decrease) in trips/tours for a given set of modes. '''
        sql_where = ' or '.join(('mode={0}'.format(mode) for mode in mode_list))
        base_trips = self.base._unsample(self.base._count_rows(table, sql_where))
        test_trips = self.test._unsample(self.test._count_rows(table, sql_where))
        new_trips = test_trips - base_trips
        pct_new_trips = new_trips / base_trips
        print ' '
        print mode_description.upper()
        print '-' * len(mode_description)
        print '{0:<25}{1:>10,.0f}'.format('Base daily {0}'.format(table.lower()), base_trips)
        print '{0:<25}{1:>10,.0f}'.format('Test daily {0}'.format(table.lower()), test_trips)
        print '{0:<25}{1:>+10,.0f} ({2:+.2%})'.format('Daily {0} change'.format(table.lower()), new_trips, pct_new_trips)
        print ' '
        return None

    # Wrapper methods for print_new_for_mode():
    def print_new_all(self, table='Trips'):
        ''' All-trips wrapper for print_new_for_mode(). '''
        return self.print_new_for_mode(xrange(1, 15), 'Change in Total {0}'.format(table), table)
    def print_new_auto(self, table='Trips'):
        ''' Auto-trips wrapper for print_new_for_mode(). '''
        return self.print_new_for_mode(xrange(1, 7), 'Change in Auto {0}'.format(table), table)
    def print_new_dtt(self, table='Trips'):
        ''' Drive-to-transit wrapper for print_new_for_mode(). '''
        return self.print_new_for_mode([11, 12], 'Change in Drive-to-Transit {0}'.format(table), table)
    def print_new_wtt(self, table='Trips'):
        ''' Walk-to-transit wrapper for print_new_for_mode(). '''
        return self.print_new_for_mode([9, 10], 'Change in Walk-to-Transit {0}'.format(table), table)
    def print_new_other(self, table='Trips'):
        ''' Walk/bike/taxi/school bus trips wrapper for print_new_for_mode(). '''
        return self.print_new_for_mode([7, 8, 13, 14], 'Change in Non-Auto, Non-Transit {0}'.format(table), table)

    def print_ptrips_by_class_change(self):
        ''' Print the change in transit person-trips by user class. '''
        print ' '
        print 'CHANGE IN TRANSIT PERSON-TRIPS BY USER CLASS'
        print '--------------------------------------------'
        total_base_ptrips = sum(self.base.ptrips_by_class.itervalues())
        total_test_ptrips = sum(self.test.ptrips_by_class.itervalues())
        total_ptrips_diff = total_test_ptrips - total_base_ptrips
        total_pct_diff = total_ptrips_diff / total_base_ptrips
        for uclass in sorted(self.base.ptrips_by_class.keys()):
            base_ptrips = self.base.ptrips_by_class[uclass]
            test_ptrips = self.test.ptrips_by_class[uclass]
            ptrips_diff = test_ptrips - base_ptrips
            ptrips_pct_diff = ptrips_diff / total_base_ptrips
            print '{0:<25}{1:>+10,.0f} ({2:+.2%})'.format('User Class {0}'.format(uclass), ptrips_diff, ptrips_pct_diff)
        print '{0:<25}{1:>+10,.0f} ({2:+.2%})'.format('All User Classes', total_ptrips_diff, total_pct_diff)
        print ' '
        return None

    def print_transit_stats_change(self, grouped=True):
        ''' Print the change in transit stats, by mode or grouped. '''
        def stat_txt(stat_name, mode=None, is_grouped=grouped):
            ''' Helper function to return formatted output text. '''
            if is_grouped:
                base_stat = sum(self.base.transit_stats[stat_name].itervalues())
                test_stat = sum(self.test.transit_stats[stat_name].itervalues())
            else:
                base_stat = self.base.transit_stats[stat_name][mode]
                test_stat = self.test.transit_stats[stat_name][mode]
            stat_diff = test_stat - base_stat
            stat_pct_diff = stat_diff / base_stat
            return '{0:+,.0f} ({1:+7.2%})'.format(stat_diff, stat_pct_diff)
        print ' '
        if grouped:
            print 'TRANSIT STATS CHANGE'
            print '--------------------'
            print ' {0:<20} | {1:<20} | {2:<20} '.format('Boardings', 'Pass. Miles', 'Pass. Hours')
            print '{0:-<22}|{0:-<22}|{0:-<22}'.format('')
            brd_txt = stat_txt('BOARDINGS')
            pmt_txt = stat_txt('PMT')
            pht_txt = stat_txt('PHT')
            print ' {0:>20} | {1:>20} | {2:>20} '.format(brd_txt, pmt_txt, pht_txt)
        else:
            print 'TRANSIT STATS CHANGE BY MODE'
            print '----------------------------'
            print ' {0:<20} | {1:<20} | {2:<20} | {3:<20} '.format('Mode', 'Boardings', 'Pass. Miles', 'Pass. Hours')
            print '{0:-<22}|{0:-<22}|{0:-<22}|{0:-<22}'.format('')
            for mode_code, mode_desc in sorted(ABM.transit_modes.iteritems(), key=lambda x: x[1]):
                brd_txt = stat_txt('BOARDINGS', mode_code)
                pmt_txt = stat_txt('PMT', mode_code)
                pht_txt = stat_txt('PHT', mode_code)
                print ' {0:<20} | {1:>20} | {2:>20} | {3:>20} '.format(mode_desc, brd_txt, pmt_txt, pht_txt)
        print ' '
        return None


### SCRIPT MODE ###
def main(build_db=True):
    print '\n{0:*^50}'.format(' P R O C E S S I N G ')
    print '\n{0:=^50}\n'.format(' BASE NETWORK ')
    base = ABM(r'Y:\nmp\basic_template_20140521', 0.05, build_db)
    base.print_mode_share()
    base.print_transit_stats()
    base.print_ptrips_by_class()
    print base
    print ' '

    print '\n{0:=^50}\n'.format(' TEST NETWORK ')
    test = ABM(r'Y:\nmp\basic_template_20140527', 0.05, build_db)
    test.print_mode_share()
    test.print_transit_stats()
    test.print_ptrips_by_class()
    print test
    print ' '

    print '\n{0:=^50}\n'.format(' COMPARISON ')
    comp = Comparison(base, test)
    print comp
    print ' '

    print '\n{0:*^50}'.format(' R E S U L T S ')
    comp.print_mode_share_change()
    comp.print_transit_stats_change()
    comp.print_ptrips_by_class_change()
    comp.export_persontrips_csv(os.path.join(base._TEST_DIR, 'persontrips_by_zn_o.csv'), 'zone', 'origin')
    comp.export_persontrips_csv(os.path.join(base._TEST_DIR, 'persontrips_by_zn_d.csv'), 'zone', 'destination')
    comp.export_persontrips_csv(os.path.join(base._TEST_DIR, 'persontrips_by_sz_o.csv'), 'subzone', 'origin')
    comp.export_persontrips_csv(os.path.join(base._TEST_DIR, 'persontrips_by_sz_d.csv'), 'subzone', 'destination')
    comp.print_new_all()
    comp.print_new_auto()
    comp.print_new_dtt()
    comp.print_new_wtt()
    comp.print_new_other()

    comp.close_dbs()

    return (base, test, comp)

if __name__ == '__main__':
    main()
