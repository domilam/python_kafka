#!/usr/bin/env python
# -*- coding: utf-8 -*-
import site

site.addsitedir('/opt/projects/applications/edf_aloe_sup/')

import pandas as pd
import numpy as np
import datetime
from mysqlalchemy import Mysqlalchemy
import calendar
import random
from mypymongo import MyPyMongo
from esquantity import EsQuantity


class UtilsSp:
    def __init__(self, file_to_transfert, name_datasheet):
        self._file = file_to_transfert
        self._name_datasheet = name_datasheet
        self._df = None
        self._conn = None
        self._base = None
        self._session = None
        self._Batiments = None
        self._Mesures = None
        self._Zones = None
        self._list_datas = []
        self._collection_mesures = 'mesures'
        self._db_mg = None
        self._con_mg = None
        self._company = {}
        self._db_mg = None
        self._con_mg = None
        self._datas = None
        self._pv_size = 150  # Kwc
        self._date_encours = None
        self._collection_company = 'companies'
        # self._list_equipements = ['ADMI/RDC', 'ADMI/ET1', 'ADMI/ET2', 'ADMI/GFR', 'ADMI/GF1', 'ADMI/GF2',
        #                           'COMM/ET1',
        #                           'SHOW/RDC', 'SHOW/GFR',
        #                           'REST/RDC',
        #                           'ECLA/',
        #                           'VEPU/',
        #                           'VEPR/',
        #                           'PV/',
        #                           'BATT/']
        # self._list_equipements = ['ADMI/RDC', 'ADMI/ET1', 'ADMI/GF1',
        #                           'SHOW/RDC', 'SHOW/GF1',
        #                           'PV/',
        #                           'BATT/']
        self._list_equipements = ['ADMI/ET2', 'ADMI/GFR', 'ADMI/GF2',
                                  'COMM/ET1',
                                  'SHOW/GFR',
                                  'REST/RDC',
                                  'ECLA/',
                                  'VEPU/',
                                  'VEPR/']

    def build_datas_fake(self, yr, base_auth=None, company=None):
        """
        import datas from xlsx
        """
        if self._file.endswith('xls') or self._file.endswith('xlsx'):
            try:
                self._df = pd.read_excel(self._file, self._name_datasheet)
            except FileNotFoundError as e:
                mess = "this excel file doesn't exist : {}".format(e)
                print(mess)
                return False, mess
            self.extend_data_mg(yr, base_auth=base_auth, company=company)
        return

    def build_pv_fake(self, yr, base_auth=None, company=None):
        # self._list_datas = []
        pas = EsQuantity(1, 'second')
        pas = pas.to('hour')
        pas = pas.value
        mymongo = MyPyMongo()
        if base_auth is None:
            base_auth = 'aloe_auth'
        if company is None:
            company = 'EDF'
        con_auth, db_auth = mymongo(base_auth)
        print(db_auth)
        result = db_auth['companies'].find_one({'company': company})
        self._con_mg, self._db_mg = mymongo(company + '_ALOE_' + 'DB_' + str(result['_id']))
        self._datas = self._db_mg['pv'].find({'_id': {'$regex': str(yr)}}, no_cursor_timeout=True).sort('date',1)

        for row in self._datas:
            puissance_m = []
            list_p = np.array(row['power_ratio']) * self._pv_size
            list_p = list_p.tolist()
            group = 'PV'
            dy = str(row['_id'])
            self._date_encours = datetime.datetime.strptime(dy, '%Y-%m-%d')
            date_encours = self._date_encours.date()
            iso_date = date_encours.isoformat()
            cle = group + '//' + dy
            i = 0
            for seconds in range(0, 86400):
                m, s = divmod(seconds, 60)
                h, m = divmod(m, 60)
                time_encours = datetime.time(h, m, s)
                datetime_encours = datetime.datetime.combine(self._date_encours, time_encours)
                puissance_m.append([datetime_encours, list_p[i]])
                i += 1

            document = {'_id': cle, "puissance_m": puissance_m, 'pas': pas, 'unit_pas': 'hour',
                        'date': iso_date}
            # self._list_datas.append(document)
            try:
                self._db_mg[self._collection_mesures].update_one({'_id': cle}, {'$set': document},
                                                                 upsert=True)
            except OSError as e:
                pass
            print('{}'.format(self._date_encours))
        self._datas.close()
        self._con_mg.close()

    def extend_data(self, param_year, df=None):
        mysql = Mysqlalchemy()
        self._base, self._session = mysql('edf_supervision', type_db="mysql+mysqlconnector")
        self._Batiments = self._base.classes.batiments
        self._Mesures = self._base.classes.mesures
        self._Zones = self._base.classes.zones_conso
        if df is None:
            df = self._df
        try:
            dates = df['Date']
            heures = df['Heure']
            ptws = df['PTW']
        except KeyError as e:
            message = 'Les noms des champs ne correspondent pas dans la {}: {}'.format(self._name_datasheet, e)
            print(message)
            return message
        self.build_year(param_year)
        self._session.close()

    def extend_data_mg(self, param_year, base_auth=None, company=None):
        mymongo = MyPyMongo()
        if base_auth is None:
            base_auth = 'aloe_auth'
        if company is None:
            company = 'EDF'
        con_auth, db_auth = mymongo(base_auth)
        print(db_auth)
        result = db_auth['companies'].find_one({'company': company})
        self._con_mg, self._db_mg = mymongo(company + '_ALOE_' + 'DB_' + str(result['_id']))
        # if df is None:
        #     df = self._df
        # try:
        #     dates = df['Date']
        #     heures = df['Heure']
        #     ptws = df['PTW']
        # except KeyError as e:
        #     message = 'Les noms des champs ne correspondent pas dans la {}: {}'.format(self._name_datasheet,e)
        #     print(message)
        #     return message
        self.build_year_mg(param_year)
        self._con_mg.close()

    def build_year(self, y):
        for month in range(1, 13):
            weeks = calendar.monthcalendar(2017, month)
            for week in weeks:
                for day in week:
                    if day != 0:
                        self._list_datas = []
                        date_encours = datetime.date(y, month, day)
                        i = 0
                        coef = random.randint(2, 6)
                        for seconds in range(0, 86400):
                            m, s = divmod(seconds, 60)
                            h, m = divmod(m, 60)
                            time_encours = datetime.time(h, m, s)
                            datetime_encours = datetime.datetime.combine(date_encours, time_encours)
                            enreg = self._Mesures(id_zone=1, dateheure=datetime_encours, date_m=date_encours,
                                                  heure_m=time_encours, puissance_m=float(self._df['PTW'][i] * coef))
                            self._list_datas.append(enreg)
                            i += 1
                            print('{} - {}'.format(date_encours, time_encours))
                        self._session.add_all(self._list_datas)
                        self._session.commit()

    def build_year_mg(self, y):
        pas = EsQuantity(1, 'second')
        pas = pas.to('hour')
        pas = pas.value
        message = ''
        for group in self._list_equipements:
            for month in range(1, 13):
                weeks = calendar.monthcalendar(2017, month)
                for week in weeks:
                    for day in week:
                        if day != 0:
                            self._list_datas = []
                            puissance_m = []
                            date_encours = datetime.date(y, month, day)
                            iso_date = date_encours.isoformat()
                            i = 0
                            coef = random.randint(2, 6)
                            cle = group + '/' + date_encours.strftime('%Y-%m-%d')
                            for seconds in range(0, 86400):
                                m, s = divmod(seconds, 60)
                                h, m = divmod(m, 60)
                                time_encours = datetime.time(h, m, s)
                                datetime_encours = datetime.datetime.combine(date_encours, time_encours)
                                puissance_m.append([datetime_encours, float(self._df['PTW'][i] * coef)])
                                i += 1
                                print('{} - {}'.format(date_encours, time_encours))
                            document = {'_id': cle, "puissance_m": puissance_m, 'pas': pas, 'unit_pas': 'hour',
                                        'date': iso_date}
                            self._list_datas.append(document)
                            try:
                                self._db_mg[self._collection_mesures].update_one({'_id': cle}, {'$set': document},
                                                                                 upsert=True)
                            except OSError as e:
                                message = 'Error Datas has not been insert in database: {}'.format(e)
                                return None, message
                            message = "Datas has been inserted"
        return message

    def add_date_in_stat(self, coll_stat):
        """
        add field date in a collection stat from id date string
        :param coll_stat: collection statistique: string
        """
        list_id = []
        mymongo = MyPyMongo()
        con_auth, db_auth = mymongo('aloe_auth')
        print(db_auth)
        result = db_auth['companies'].find_one({'company': 'EDF'})
        self._con_mg, self._db_mg = mymongo('EDF' + '_ALOE_' + 'DB_' + str(result['_id']))
        stats = self._db_mg[coll_stat].find({})
        for stat in stats:
            list_id.append(stat['_id'])
        for id_stat in list_id:
            split_id = id_stat.split("/")
            date_str = split_id[len(split_id)-1]
            date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
            dateiso = date_obj.isoformat()
            self._db_mg[coll_stat].update_one({'_id': id_stat}, {'$set': {'date': dateiso}})
        self._con_mg.close()




if __name__ == '__main__':
    utils_sp = UtilsSp('/home/dominique/esimsbess/datas_test.xlsx', 'Feuil1')
    # d = utils_sp.build_datas_fake(2017, base_auth='aloe_auth', company='EDF')
    utils_sp.build_pv_fake(2017, base_auth='aloe_auth', company='EDF')
    # utils_sp.add_date_in_stat('statheure')
