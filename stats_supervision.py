#!/usr/bin/env python
# -*- coding: utf-8 -*-
# import site
#
# site.addsitedir('/opt/projects/applications/edf_aloe_sup/')

from mypymongo import MyPyMongo
import datetime
from esdate import validate_date
import numpy as np
import time
import dateutil.parser
from esmessage import EsMessage
from esquantity import EsQuantity
import dateutil.parser
import re
import warnings

class StatsSupervision:
    def __init__(self):
        self._start_time = None
        self._con_mg = None
        self._db_mg = None
        self._collection_mesures = 'mesures'
        self._collection_pv = 'pv'
        self._dict_mesures = {}
        self._dict_datas = {}
        self._dict_test_datas = {}
        self._dict_groups = {}
        self._date_encours = None
        self._pv = False
        self._coef = 1
        # self._pv_size = 150  # Kwc
        self._dict_sum = {}
        self._equipement = None
        self._clk15m = None
        self._clkheure = None
        # TODO uncomment this lines
        # self._list_equipements = ['ADMI/RDC', 'ADMI/ET1', 'ADMI/ET2', 'ADMI/GFR', 'ADMI/GF1', 'ADMI/GF2',
        #                           'COMM/ET1',
        #                           'SHOW/RDC', 'SHOW/GFR',
        #                           'REST/RDC',
        #                           'ECLA/',
        #                           'VEPU/',
        #                           'VEPR/',
        #                           'PV/',
        #                           'BATT/']
        self._list_equipements = ['ADMI/RDC', 'ADMI/ET1']
        self._pas = None
        self._pas_datas = 1 / 60 / 60
        self._datas = None
        # self._collections_stat = ['stat15m', 'statheure', 'statjour']
        self._collections_stat = ['statjour']
        self._collection_stat = None
        self._list_stat = ['p_max', 'p_min', 'p_mean', 'energie_diurne', 'energie_nocturne', 'energie_pointes',
                           'energie_creuses', 'energie_pleines', 'energie_heures']
        self._list_graph = []
        p_max_color = "#FF0022"
        p_moy_color = "#BB878E"
        p_min_color = "#15FF45"
        e_diurne_color = "#F7FF16"
        e_nocturne_color = "#160F7F"
        e_pointe_color = "#3C15E8"
        e_creuse_color = "15A0E8"
        # e_pleine_color = "#183AFF"
        e_pleine_color = "#7B86CC"
        self._color = [p_max_color, p_min_color, p_moy_color, e_diurne_color, e_nocturne_color, e_pointe_color,
                       e_creuse_color, e_pleine_color]

        self._diur_inf = time_to_seconds('06:00:00')
        self._diur_sup = time_to_seconds('17:59:59')
        self._noct_inf1 = time_to_seconds('00:00:00')
        self._noct_sup1 = time_to_seconds('05:59:59')
        self._noct_inf2 = time_to_seconds('18:00:00')
        self._noct_sup2 = time_to_seconds('23:59:59')
        self._plei_inf1 = None
        self._plei_sup1 = None
        self._plei_inf2 = None
        self._plei_sup2 = None
        self._plei_inf3 = None
        self._plei_sup3 = None
        self._creu_inf1 = None
        self._creu_sup1 = None
        self._creu_inf2 = None
        self._creu_sup2 = None
        self._poin_inf1 = None
        self._poin_sup1 = None
        self._poin_inf2 = None
        self._poin_sup2 = None

    def __call__(self, company='EDF', db_auth_name='aloe_auth', date='', pas=900):
        self._date = date
        self._pas = pas
        self._company = company
        self._db_auth_name = db_auth_name
        self.connect_db()

    def get_db(self):
        return self._db_mg

    def get_datas(self):
        return self._datas

    def get_test_datas(self):
        return self._dict_test_datas

    def get_dict_datas(self):
        return self._dict_datas

    def set_list_equipements(self, list_equipements):
        self._list_equipements = list_equipements

    def set_date(self, dt):
        self._date = dt

    def set_collection_mesures(self, coll):
        self._collection_mesures = coll

    def connect_db(self):
        mymongo = MyPyMongo()
        con_auth, db_auth = mymongo(self._db_auth_name)
        # print(db_auth)
        result = db_auth['companies'].find_one({'company': self._company})
        self._con_mg, self._db_mg = mymongo(self._company + '_ALOE_' + 'DB_' + str(result['_id']))
        return self._con_mg, self._db_mg

    def get_day_series(self, date_deb='2017-01-01', date_fin='2017-03-31'):
        """
        get rows of date_jour in mesures collections
        :param date_fin:
        :param date_deb:
        :return: series
        """
        my_message = EsMessage()
        rows_mesures = None
        # try:
        #     self._con_mg.close()
        # except AttributeError:
        #     pass
        # self.connect_db()
        confirm, datej, date_obj = validate_date(self._date)
        if confirm or self._date == '':
            # detailgp = self._equipement.split('/')
            # if detailgp[0] == 'PV':
            #     cle = self._date
            #     collection = self._collection_pv
            # else:
            cle = self._equipement + '/' + self._date
            collection = self._collection_mesures

            try:
                # if cle == '' and collection == self._collection_pv:
                #     rows_mesures = self._db_mg[collection].find({'date': {'$gte': '2017-01-01', '$lte': '2017-03-31'}},
                #                                                 no_cursor_timeout=True)
                # else:
                rows_mesures = self._db_mg[collection].find(
                    {'_id': {'$regex': cle}, 'date': {'$gte': date_deb, '$lte': date_fin}},
                    no_cursor_timeout=True).sort('date', 1)
            except OSError as e:
                message = 'Error: Datas was not found in database: {}'.format(e)
                type_message = 'warning'
                message_format = my_message.get_message(message, type_message)
                # self._con_mg.close()
                return None, message_format
            message = "OK"
            type_message = 'success'
        else:
            message = "Date is not valid"
            type_message = 'warning'
        message_format = my_message.get_message(message, type_message)
        # self._con_mg.close()
        return rows_mesures, message_format

    def build_stat_mesures(self, collection_st, equipement=None):
        """
        build stat pas 15 minutes, stat pas heure, stat pas jour
        """
        type_message = None
        message = None
        if equipement is not None:
            self._equipement = equipement

        series, message = self.get_day_series()
        for row in series:

            # if self._equipement == 'PV/':
            #     group = 'PV'
            #     dy = str(row['_id'])
            #     self._date_encours = datetime.datetime.strptime(dy, '%Y-%m-%d')
            #     p_list = np.array(row['power_ratio']) * self._pv_size
            # else:
            list_id = row['_id'].split('/')
            group = list_id[0] + '/' + list_id[1]
            dy = str(row['date'])
            if row['_id'][:2] == 'PV':
                self._pv = True
            self._date_encours = dateutil.parser.parse(row['date'])
            p_list = row['puissance_m']
            self._date_encours = self._date_encours.date()
            dateiso = self._date_encours.isoformat()
            cle_stat = group + '/' + dy
            p_max_pas, p_min_pas, p_mean_pas, e_allhours_pas, e_pointes_pas, e_nocturne_pas, e_pleines_pas, \
            e_creuses_pas, e_diurne_pas, lp_pointes, lp_creuses, lp_pleines, lp_nocturne, lp_diurne = self.pstat(p_list)
            if cle_stat in self._dict_datas.keys():
                if 'p_max' not in self._dict_datas[cle_stat].keys():
                    self._dict_datas[cle_stat] = {'date': dateiso, 'p_max': p_max_pas, 'p_min': p_min_pas,
                                                  'p_mean': p_mean_pas}
                    self._dict_test_datas[cle_stat] = dict(date=dateiso, p_pointes=lp_pointes, p_creuses=lp_creuses,
                                                           p_pleines=lp_pleines, p_nocturne=lp_nocturne,
                                                           p_diurne=lp_diurne)
            else:
                self._dict_datas[cle_stat] = dict(date=dateiso, p_max=p_max_pas, p_min=p_min_pas, p_mean=p_mean_pas,
                                                  energie_heures=e_allhours_pas, energie_pointes=e_pointes_pas,
                                                  energie_nocturne=e_nocturne_pas, energie_pleines=e_pleines_pas,
                                                  energie_creuses=e_creuses_pas, energie_diurne=e_diurne_pas)

                self._dict_test_datas[cle_stat] = dict(date=dateiso, p_pointes=lp_pointes, p_creuses=lp_creuses,
                                                       p_pleines=lp_pleines, p_nocturne=lp_nocturne,
                                                       p_diurne=lp_diurne)

            document = self._dict_datas[cle_stat]
            try:
                self._db_mg[collection_st].update_one({'_id': cle_stat}, {'$set': document}, upsert=True)
                # print(cle_stat)
                message = "Datas has been inserted"
                type_message = "success"
            except OSError as e:
                message = 'Error Datas has not been insert in database: {}'.format(e)
                type_message = "warning"
                # self._con_mg.close()
                return message, type_message

        series.close()
        return message, type_message

    def build_stat_mesures_stream(self, collection_st, equipement=None, dated=None, datef=None):
        """
        build stat pas 15 minutes, stat pas heure, stat pas jour
        """
        type_message = None
        message = None
        if equipement is not None:
            self._equipement = equipement

        if dated is None or datef is None:
            dated = self._date_encours.strftime('%Y-%m-%d')
            datef = self._date_encours.strftime('%Y-%m-%d')
        series, message = self.get_day_series(date_deb=dated, date_fin=datef)
        for row in series:
            list_id = row['_id'].split('/')
            group = list_id[0] + '/' + list_id[1]
            dy = str(row['date'])
            if row['_id'][:2] == 'PV':
                self._pv = True
            self._date_encours = dateutil.parser.parse(row['date'])
            p_list = row['puissance_m']
            # TODO CREATE ADD_INEXISTANT_VALUE
            p_list = self.add_inexistant_values(p_list, collection_statistique=collection_st)
            self._date_encours = self._date_encours.date()
            dateiso = self._date_encours.isoformat()
            cle_stat = group + '/' + dy
            p_max_pas, p_min_pas, p_mean_pas, e_allhours_pas, e_pointes_pas, e_nocturne_pas, e_pleines_pas, \
            e_creuses_pas, e_diurne_pas, lp_pointes, lp_creuses, lp_pleines, lp_nocturne, lp_diurne = self.pstat_stream(p_list)
            if cle_stat in self._dict_datas.keys():
                if 'p_max' not in self._dict_datas[cle_stat].keys():
                    self._dict_datas[cle_stat] = {'date': dateiso, 'p_max': p_max_pas, 'p_min': p_min_pas,
                                                  'p_mean': p_mean_pas}
                    self._dict_test_datas[cle_stat] = dict(date=dateiso, p_pointes=lp_pointes, p_creuses=lp_creuses,
                                                           p_pleines=lp_pleines, p_nocturne=lp_nocturne,
                                                           p_diurne=lp_diurne)
            else:
                self._dict_datas[cle_stat] = dict(date=dateiso, p_max=p_max_pas, p_min=p_min_pas, p_mean=p_mean_pas,
                                                  energie_heures=e_allhours_pas, energie_pointes=e_pointes_pas,
                                                  energie_nocturne=e_nocturne_pas, energie_pleines=e_pleines_pas,
                                                  energie_creuses=e_creuses_pas, energie_diurne=e_diurne_pas)

                self._dict_test_datas[cle_stat] = dict(date=dateiso, p_pointes=lp_pointes, p_creuses=lp_creuses,
                                                       p_pleines=lp_pleines, p_nocturne=lp_nocturne,
                                                       p_diurne=lp_diurne)

            document = self._dict_datas[cle_stat]
            try:
                self._db_mg[collection_st].update_one({'_id': cle_stat}, {'$set': document}, upsert=True)
                # print(cle_stat)
                message = "Datas has been inserted"
                type_message = "success"
            except OSError as e:
                message = 'Error Datas has not been insert in database: {}'.format(e)
                type_message = "warning"
                # self._con_mg.close()
                return message, type_message

        series.close()
        return message, type_message

    def pstat(self, list_puissance, pas=None, date=None):
        """
        retourne le max, la moyenne d'une liste de couple [datetime, valeur] sur un pas de temps
        :param list_puissance: liste de couple [datetime, valeur]: list
        :return: puissances_max_pas: liste des puissances max par pas de temps: list
        :return: list_p_moy_pas: listes des puissances myennes par pas de temps: list
        """
        if not self._pv:
            self._coef = 1 / 1000
        else:
            self._coef = 1

        if pas is not None and date is not None:
            # only for unit test
            self._pas = pas
            self._date_encours = date
        # puissances = np.array([])
        list_e_diurne = []
        list_e_nocturne = []
        list_e_creuses = []
        list_e_pleines = []
        list_e_pointes = []
        list_e_all_hours = []
        list_p_moy_pas = np.array([])
        list_p_max_pas = np.array([])
        list_p_min_pas = np.array([])
        list_p_diurne = np.array([])
        list_p_nocturne = np.array([])
        list_p_creuses = np.array([])
        list_p_pleines = np.array([])
        list_p_pointes = np.array([])
        list_p = np.array([])
        interval_reached = False
        i = 0
        day_week = self._date_encours.weekday()
        if day_week == 5 or day_week == 6:
            self._plei_inf1 = time_to_seconds('07:00:00')
            self._plei_sup1 = time_to_seconds('20:59:59')
            self._creu_inf1 = time_to_seconds('00:00:00')
            self._creu_sup1 = time_to_seconds('06:59:59')
            self._creu_inf2 = time_to_seconds('21:00:00')
            self._creu_sup2 = time_to_seconds('23:59:59')
            weekend = True
        else:
            self._plei_inf1 = time_to_seconds('07:00:00')
            self._plei_sup1 = time_to_seconds('07:59:59')
            self._plei_inf2 = time_to_seconds('13:00:00')
            self._plei_sup2 = time_to_seconds('16:59:59')
            self._plei_inf3 = time_to_seconds('20:00:00')
            self._plei_sup3 = time_to_seconds('20:59:59')
            self._creu_inf1 = time_to_seconds('00:00:00')
            self._creu_sup1 = time_to_seconds('06:59:59')
            self._creu_inf2 = time_to_seconds('21:00:00')
            self._creu_sup2 = time_to_seconds('23:59:59')
            self._poin_inf1 = time_to_seconds('08:00:00')
            self._poin_sup1 = time_to_seconds('12:59:59')
            self._poin_inf2 = time_to_seconds('17:00:00')
            self._poin_sup2 = time_to_seconds('19:59:59')
            weekend = False
        if self._pas == 86400:
            list_p_diurne = np.array(list_puissance[self._diur_inf: self._diur_sup + 1])
            list_p_nocturne = np.array(list_puissance[self._noct_inf1: self._noct_sup1 + 1] + list_puissance[
                                                                                              self._noct_inf2:self._noct_sup2 + 1])
            if weekend:
                list_p_pleines = np.array(list_puissance[self._plei_inf1:self._plei_sup1 + 1])
                list_p_creuses = np.array(list_puissance[self._creu_inf1:self._creu_sup1 + 1] + list_puissance[
                                                                                                self._creu_inf2:self._creu_sup2 + 1])

            # if not saturday or sunday
            else:
                list_p_pleines = np.array(list_puissance[self._plei_inf1:self._plei_sup1 + 1] + list_puissance[
                                                                                                self._plei_inf2:self._plei_sup2 + 1] + list_puissance[
                                                                                                                                       self._plei_inf3:self._plei_sup3 + 1])

                list_p_creuses = np.array(list_puissance[self._creu_inf1:self._creu_sup1 + 1] + list_puissance[
                                                                                                self._creu_inf2:self._creu_sup2 + 1])
                list_p_pointes = np.array(list_puissance[self._poin_inf1:self._poin_sup1 + 1] + list_puissance[
                                                                                                self._poin_inf2:self._poin_sup2 + 1])

            dtetime = list_puissance[len(list_puissance) - 1][0]
            dtetime = dtetime.isoformat()
            list_p = np.array(list_puissance)
            p_max = np.max(list_p.take(1, axis=1)) * self._coef
            p_min = np.min(list_p.take(1, axis=1)) * self._coef
            p_moy = np.mean(list_p.take(1, axis=1)) * self._coef
            list_p_max_pas = [[dtetime, p_max]]
            list_p_min_pas = [[dtetime, p_min]]
            list_p_moy_pas = [[dtetime, p_moy]]

            list_e_all_hours.append(
                [dtetime, get_energy(list_p.take(1, axis=1).tolist(), self._pas_datas) * self._coef])

            energie_diurne = get_energy(list_p_diurne.take(1, axis=1).tolist(), self._pas_datas) * self._coef
            list_e_diurne.append([dtetime, energie_diurne])

            energie_nocturne = get_energy(list_p_nocturne.take(1, axis=1).tolist(), self._pas_datas) * self._coef
            list_e_nocturne.append([dtetime, energie_nocturne])

            energie_heures_pleines = get_energy(list_p_pleines.take(1, axis=1).tolist(), self._pas_datas) * self._coef
            list_e_pleines.append([dtetime, energie_heures_pleines])

            energie_heures_creuses = get_energy(list_p_creuses.take(1, axis=1).tolist(), self._pas_datas) * self._coef
            list_e_creuses.append([dtetime, energie_heures_creuses])
            if day_week != 5 and day_week != 6:
                energie_pointe = get_energy(list_p_pointes.take(1, axis=1).tolist(), self._pas_datas) * self._coef
                list_e_pointes.append([dtetime, energie_pointe])

        elif self._pas == 3600 or self._pas == 900:
            if self._pas == 3600:
                x_limite = 24
            else:
                x_limite = 96
            i = 0
            for h in range(0, x_limite):
                self._date_encours = list_puissance[i][0]
                if h == x_limite - 1:
                    dtetime = list_puissance[i + self._pas - 1][0]
                else:
                    dtetime = list_puissance[i + self._pas][0]
                dtetime = dtetime.isoformat()

                list_p = np.array(list_puissance[i:i + self._pas])
                p_max = np.max(list_p.take(1, axis=1)) * self._coef
                p_min = np.min(list_p.take(1, axis=1)) * self._coef
                p_moy = np.mean(list_p.take(1, axis=1)) * self._coef

                if not list_p_max_pas:
                # if list_p_max_pas.size == 0:
                    list_p_max_pas = np.array([[dtetime, p_max]])
                else:
                    list_p_max_pas = np.append(list_p_max_pas, np.array([[dtetime, p_max]]), axis=0)

                if not list_p_min_pas:
                # if list_p_min_pas.size == 0:
                    list_p_min_pas = np.array([[dtetime, p_min]])
                else:
                    list_p_min_pas = np.append(list_p_min_pas, np.array([[dtetime, p_min]]), axis=0)

                if not list_p_moy_pas:
                # if list_p_moy_pas.size == 0:
                    list_p_moy_pas = np.array([[dtetime, p_moy]])
                else:
                    list_p_moy_pas = np.append(list_p_moy_pas, np.array([[dtetime, p_moy]]), axis=0)

                list_e_all_hours.append(
                    [dtetime, get_energy(list_p.take(1, axis=1).tolist(), self._pas_datas) * self._coef])

                if self._diur_inf <= i <= self._diur_sup:
                    energie_diurne = get_energy(list_p.take(1, axis=1).tolist(), self._pas_datas) * self._coef
                    list_e_diurne.append([dtetime, energie_diurne])
                    list_p_diurne = np.array([list_p]) if not list_p_diurne.any() else np.append(
                        list_p_diurne, [list_p], axis=0)

                elif (self._noct_inf1 <= i <= self._noct_sup1) or (
                                self._noct_inf2 <= i <= self._noct_sup2):
                    energie_nocturne = get_energy(list_p.take(1, axis=1).tolist(), self._pas_datas) * self._coef
                    list_e_nocturne.append([dtetime, energie_nocturne])
                    list_p_nocturne = np.array([list_p]) if not list_p_nocturne.any() else np.append(
                        list_p_nocturne, [list_p], axis=0)

                # if saturday 5 or sunday 6
                if weekend:
                    if self._plei_inf1 <= i <= self._plei_sup1:
                        energie_heures_pleines = get_energy(list_p.take(1, axis=1).tolist(),
                                                            self._pas_datas) * self._coef
                        list_e_pleines.append([dtetime, energie_heures_pleines])
                        list_p_pleines = np.array([list_p]) if not list_p_pleines.any() else np.append(
                            list_p_pleines, [list_p], axis=0)

                    else:
                        energie_heures_creuses = get_energy(list_p.take(1, axis=1).tolist(),
                                                            self._pas_datas) * self._coef
                        list_e_creuses.append([dtetime, energie_heures_creuses])
                        list_p_creuses = np.array([list_p]) if not list_p_creuses.any() else np.append(
                            list_p_creuses, [list_p], axis=0)

                # if not saturday or sunday
                else:
                    # heures pleines
                    if (self._plei_inf1 <= i <= self._plei_sup1) or (
                                    self._plei_inf2 <= i <= self._plei_sup2) or (
                                    self._plei_inf3 <= i <= self._plei_sup3):
                        energie_heures_pleines = get_energy(list_p.take(1, axis=1).tolist(),
                                                            self._pas_datas) * self._coef
                        list_e_pleines.append([dtetime, energie_heures_pleines])
                        list_p_pleines = np.array([list_p]) if not list_p_pleines.any() else np.append(list_p_pleines,
                                                                                                       [list_p], axis=0)

                    # heures creuses
                    elif (self._creu_inf1 <= i <= self._creu_sup1) or (self._creu_inf2 <= i <= self._creu_sup2):
                        energie_heures_creuses = get_energy(list_p.take(1, axis=1).tolist(),
                                                            self._pas_datas) * self._coef
                        list_e_creuses.append([dtetime, energie_heures_creuses])
                        list_p_creuses = np.array([list_p]) if not list_p_creuses.any() else np.append(
                            list_p_creuses, [list_p], axis=0)

                    # heures de pointe
                    elif (self._poin_inf1 <= i <= self._poin_sup1) or (
                                    self._poin_inf2 <= i <= self._poin_sup2):
                        energie_pointe = get_energy(list_p.take(1, axis=1).tolist(), self._pas_datas) * self._coef
                        list_e_pointes.append([dtetime, energie_pointe])
                        list_p_pointes = np.array([list_p]) if not list_p_pointes.any() else np.append(
                            list_p_pointes, [list_p], axis=0)

                i += self._pas

        dt = np.dtype('U20, f')  # create a new type
        list_p_max_pas = np.array([tuple(row) for row in list_p_max_pas], dtype=dt)
        list_p_max_pas = [list(n) for n in list_p_max_pas.tolist()]
        list_p_min_pas = np.array([tuple(row) for row in list_p_min_pas], dtype=dt)
        list_p_min_pas = [list(n) for n in list_p_min_pas.tolist()]
        list_p_moy_pas = np.array([tuple(row) for row in list_p_moy_pas], dtype=dt)
        list_p_moy_pas = [list(n) for n in list_p_moy_pas.tolist()]

        list_p_diurne = list_p_diurne.tolist()
        list_p_nocturne = list_p_nocturne.tolist()
        list_p_creuses = list_p_creuses.tolist()
        list_p_pleines = list_p_pleines.tolist()
        list_p_pointes = list_p_pointes.tolist()

        return list_p_max_pas, list_p_min_pas, list_p_moy_pas, list_e_all_hours, list_e_pointes, list_e_nocturne, list_e_pleines, list_e_creuses, list_e_diurne, list_p_pointes, list_p_creuses, list_p_pleines, list_p_nocturne, list_p_diurne

    def pstat_stream(self, list_puissance, pas=None, date=None):
        """
        retourne le max, la moyenne d'une liste de couple [datetime, valeur] sur un pas de temps
        :param list_puissance: liste de couple [datetime, valeur]: list
        :return: puissances_max_pas: liste des puissances max par pas de temps: list
        :return: list_p_moy_pas: listes des puissances myennes par pas de temps: list
        """
        if not self._pv:
            self._coef = 1 / 1000
        else:
            self._coef = 1

        if pas is not None and date is not None:
            # only for unit test
            self._pas = pas
            self._date_encours = date
        # puissances = np.array([])
        list_e_diurne = []
        list_e_nocturne = []
        list_e_creuses = []
        list_e_pleines = []
        list_e_pointes = []
        list_e_all_hours = []
        # list_p_moy_pas = np.array([], dtype=np.float64)
        # list_p_max_pas = np.array([], dtype=np.float64)
        # list_p_min_pas = np.array([], dtype=np.float64)
        list_p_moy_pas = []
        list_p_max_pas = []
        list_p_min_pas = []

        list_p_diurne = np.array([], dtype=np.float64)
        list_p_nocturne = np.array([], dtype=np.float64)
        list_p_creuses = np.array([], dtype=np.float64)
        list_p_pleines = np.array([], dtype=np.float64)
        list_p_pointes = np.array([], dtype=np.float64)
        list_p = np.array([], dtype=np.float64)
        interval_reached = False
        i = 0
        day_week = self._date_encours.weekday()
        if day_week == 5 or day_week == 6:
            self._plei_inf1 = time_to_seconds('07:00:00')
            self._plei_sup1 = time_to_seconds('20:59:59')
            self._creu_inf1 = time_to_seconds('00:00:00')
            self._creu_sup1 = time_to_seconds('06:59:59')
            self._creu_inf2 = time_to_seconds('21:00:00')
            self._creu_sup2 = time_to_seconds('23:59:59')
            weekend = True
        else:
            self._plei_inf1 = time_to_seconds('07:00:00')
            self._plei_sup1 = time_to_seconds('07:59:59')
            self._plei_inf2 = time_to_seconds('13:00:00')
            self._plei_sup2 = time_to_seconds('16:59:59')
            self._plei_inf3 = time_to_seconds('20:00:00')
            self._plei_sup3 = time_to_seconds('20:59:59')
            self._creu_inf1 = time_to_seconds('00:00:00')
            self._creu_sup1 = time_to_seconds('06:59:59')
            self._creu_inf2 = time_to_seconds('21:00:00')
            self._creu_sup2 = time_to_seconds('23:59:59')
            self._poin_inf1 = time_to_seconds('08:00:00')
            self._poin_sup1 = time_to_seconds('12:59:59')
            self._poin_inf2 = time_to_seconds('17:00:00')
            self._poin_sup2 = time_to_seconds('19:59:59')
            weekend = False
        if self._pas == 86400:
            list_p_diurne = np.array(list_puissance[self._diur_inf: self._diur_sup + 1])
            list_p_nocturne = np.array(list_puissance[self._noct_inf1: self._noct_sup1 + 1] + list_puissance[
                                                                                              self._noct_inf2:self._noct_sup2 + 1])
            if weekend:
                list_p_pleines = np.array(list_puissance[self._plei_inf1:self._plei_sup1 + 1])
                list_p_creuses = np.array(list_puissance[self._creu_inf1:self._creu_sup1 + 1] + list_puissance[
                                                                                                self._creu_inf2:self._creu_sup2 + 1])

            # if not saturday or sunday
            else:
                list_p_pleines = np.array(list_puissance[self._plei_inf1:self._plei_sup1 + 1] + list_puissance[
                                                                                                self._plei_inf2:self._plei_sup2 + 1] + list_puissance[
                                                                                                                                       self._plei_inf3:self._plei_sup3 + 1])

                list_p_creuses = np.array(list_puissance[self._creu_inf1:self._creu_sup1 + 1] + list_puissance[
                                                                                                self._creu_inf2:self._creu_sup2 + 1])
                list_p_pointes = np.array(list_puissance[self._poin_inf1:self._poin_sup1 + 1] + list_puissance[
                                                                                                self._poin_inf2:self._poin_sup2 + 1])

            dtetime = list_puissance[len(list_puissance) - 1][0]
            dtetime = dtetime.isoformat()
            list_p = np.array(list_puissance)
            p_max = np.max(list_p.take(1, axis=1).astype('float64')) * self._coef
            p_max = None if np.isnan(p_max) else p_max.item()
            p_min = np.min(list_p.take(1, axis=1).astype('float64')) * self._coef
            p_min = None if np.isnan(p_min) else p_min.item()
            p_moy = np.mean(list_p.take(1, axis=1).astype('float64')) * self._coef
            p_moy = None if np.isnan(p_moy) else p_moy.item()

            list_p_max_pas = [[dtetime, p_max]]
            list_p_min_pas = [[dtetime, p_min]]
            list_p_moy_pas = [[dtetime, p_moy]]

            energie_all_hours = get_energy(list_p.take(1, axis=1).tolist(), self._pas_datas) * self._coef
            energie_all_hours = None if np.isnan(energie_all_hours) else energie_all_hours.item()
            list_e_all_hours.append([dtetime, energie_all_hours])

            energie_diurne = get_energy(list_p_diurne.take(1, axis=1).tolist(), self._pas_datas) * self._coef
            energie_diurne = None if np.isnan(energie_diurne) else energie_diurne.item()
            list_e_diurne.append([dtetime, energie_diurne])

            energie_nocturne = get_energy(list_p_nocturne.take(1, axis=1).tolist(), self._pas_datas) * self._coef
            energie_nocturne = None if np.isnan(energie_nocturne) else energie_nocturne.item()
            list_e_nocturne.append([dtetime, energie_nocturne])

            energie_heures_pleines = get_energy(list_p_pleines.take(1, axis=1).tolist(), self._pas_datas) * self._coef
            energie_heures_pleines = None if np.isnan(energie_heures_pleines) else energie_heures_pleines.item()
            list_e_pleines.append([dtetime, energie_heures_pleines])

            energie_heures_creuses = get_energy(list_p_creuses.take(1, axis=1).tolist(), self._pas_datas) * self._coef
            energie_heures_creuses = None if np.isnan(energie_heures_creuses) else energie_heures_creuses.item()
            list_e_creuses.append([dtetime, energie_heures_creuses])

            if day_week != 5 and day_week != 6:
                energie_pointe = get_energy(list_p_pointes.take(1, axis=1).tolist(), self._pas_datas) * self._coef
                energie_pointe = None if np.isnan(energie_pointe) else energie_pointe.item()
                list_e_pointes.append([dtetime, energie_pointe])

        elif self._pas == 3600 or self._pas == 900:
            if self._pas == 3600:
                x_limite = time_to_seconds(self._clkheure) / self._pas
            else:
                x_limite = time_to_seconds(self._clk15m) / self._pas
            i = 0
            for h in range(0, int(x_limite)):
                self._date_encours = list_puissance[i][0]
                if h == x_limite - 1:
                    # TODO for debug
                    if self._pas == 3600:
                        print('i:{}- h:{} - pas: {}'.format(i, h, self._pas))
                    dtetime = list_puissance[i + self._pas - 1][0]
                else:
                    dtetime = list_puissance[i + self._pas][0]
                dtetime = dtetime.isoformat()

                list_p = np.array(list_puissance[i:i + self._pas])

                # I expect to see RuntimeWarnings in this block
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=RuntimeWarning)
                    p_max = np.nanmax(list_p.take(1, axis=1).astype('float64')) * self._coef
                    p_min = np.nanmin(list_p.take(1, axis=1).astype('float64')) * self._coef
                    p_moy = np.nanmean(list_p.take(1, axis=1).astype('float64')) * self._coef
                p_max = None if np.isnan(p_max) else p_max.item()
                p_min = None if np.isnan(p_min) else p_min.item()
                p_moy = None if np.isnan(p_moy) else p_moy.item()

                if not list_p_max_pas:
                # if list_p_max_pas.size == 0:
                    # list_p_max_pas = np.array([[dtetime, p_max]])
                    list_p_max_pas = [[dtetime, p_max]]
                else:
                    # list_p_max_pas = np.append(list_p_max_pas, np.array([[dtetime, p_max]]), axis=0)
                    list_p_max_pas.append([dtetime, p_max])

                if not list_p_min_pas:
                # if list_p_min_pas.size == 0:
                    # list_p_min_pas = np.array([[dtetime, p_min]])
                    list_p_min_pas = [[dtetime, p_min]]
                else:
                    # list_p_min_pas = np.append(list_p_min_pas, np.array([[dtetime, p_min]]), axis=0)
                    list_p_min_pas.append([dtetime, p_min])

                if not list_p_moy_pas:
                # if list_p_moy_pas.size == 0:
                    # list_p_moy_pas = np.array([[dtetime, p_moy]])
                    list_p_moy_pas = [[dtetime, p_moy]]

                else:
                    # list_p_moy_pas = np.append(list_p_moy_pas, np.array([[dtetime, p_moy]]), axis=0)
                    list_p_moy_pas.append([dtetime, p_moy])

                energie_all_hours = get_energy(list_p.take(1, axis=1).tolist(), self._pas_datas) * self._coef
                energie_all_hours = None if np.isnan(energie_all_hours) else energie_all_hours.item()
                list_e_all_hours.append([dtetime, energie_all_hours])

                if self._diur_inf <= i <= self._diur_sup:
                    energie_diurne = get_energy(list_p.take(1, axis=1).tolist(), self._pas_datas) * self._coef
                    energie_diurne = None if np.isnan(energie_diurne) else energie_diurne.item()
                    list_e_diurne.append([dtetime, energie_diurne])
                    list_p_diurne = np.array([list_p]) if not list_p_diurne.any() else np.append(
                        list_p_diurne, [list_p], axis=0)

                elif (self._noct_inf1 <= i <= self._noct_sup1) or (
                                self._noct_inf2 <= i <= self._noct_sup2):
                    energie_nocturne = get_energy(list_p.take(1, axis=1).tolist(), self._pas_datas) * self._coef
                    energie_nocturne = None if np.isnan(energie_nocturne) else energie_nocturne.item()
                    list_e_nocturne.append([dtetime, energie_nocturne])
                    list_p_nocturne = np.array([list_p]) if not list_p_nocturne.any() else np.append(
                        list_p_nocturne, [list_p], axis=0)

                # if saturday 5 or sunday 6
                if weekend:
                    if self._plei_inf1 <= i <= self._plei_sup1:
                        energie_heures_pleines = get_energy(list_p.take(1, axis=1).tolist(),
                                                            self._pas_datas) * self._coef
                        energie_heures_pleines = None if np.isnan(energie_heures_pleines) else energie_heures_pleines.item()
                        list_e_pleines.append([dtetime, energie_heures_pleines])
                        list_p_pleines = np.array([list_p]) if not list_p_pleines.any() else np.append(
                            list_p_pleines, [list_p], axis=0)

                    else:
                        energie_heures_creuses = get_energy(list_p.take(1, axis=1).tolist(),
                                                            self._pas_datas) * self._coef
                        energie_heures_creuses = None if np.isnan(energie_heures_creuses) else energie_heures_creuses.tem()
                        list_e_creuses.append([dtetime, energie_heures_creuses])
                        list_p_creuses = np.array([list_p]) if not list_p_creuses.any() else np.append(
                            list_p_creuses, [list_p], axis=0)

                # if not saturday or sunday
                else:
                    # heures pleines
                    if (self._plei_inf1 <= i <= self._plei_sup1) or (
                                    self._plei_inf2 <= i <= self._plei_sup2) or (
                                    self._plei_inf3 <= i <= self._plei_sup3):
                        energie_heures_pleines = get_energy(list_p.take(1, axis=1).tolist(),
                                                            self._pas_datas) * self._coef
                        energie_heures_pleines = None if np.isnan(energie_heures_pleines) else energie_heures_pleines.item()
                        list_e_pleines.append([dtetime, energie_heures_pleines])
                        list_p_pleines = np.array([list_p]) if not list_p_pleines.any() else np.append(list_p_pleines,
                                                                                                       [list_p], axis=0)

                    # heures creuses
                    elif (self._creu_inf1 <= i <= self._creu_sup1) or (self._creu_inf2 <= i <= self._creu_sup2):
                        energie_heures_creuses = get_energy(list_p.take(1, axis=1).tolist(),
                                                            self._pas_datas) * self._coef
                        energie_heures_creuses = None if np.isnan(energie_heures_creuses) else energie_heures_creuses.item()
                        list_e_creuses.append([dtetime, energie_heures_creuses])
                        list_p_creuses = np.array([list_p]) if not list_p_creuses.any() else np.append(
                            list_p_creuses, [list_p], axis=0)

                    # heures de pointe
                    elif (self._poin_inf1 <= i <= self._poin_sup1) or (
                                    self._poin_inf2 <= i <= self._poin_sup2):
                        energie_pointe = get_energy(list_p.take(1, axis=1).tolist(), self._pas_datas) * self._coef
                        energie_pointe = None if np.isnan(energie_pointe) else energie_pointe.item()
                        list_e_pointes.append([dtetime, energie_pointe])
                        list_p_pointes = np.array([list_p]) if not list_p_pointes.any() else np.append(
                            list_p_pointes, [list_p], axis=0)

                i += self._pas


        dt = np.dtype('U20, f')  # create a new type
        # list_p_max_pas = np.array([tuple(row) for row in list_p_max_pas], dtype=dt)
        # list_p_max_pas = [list(n) for n in list_p_max_pas.tolist()]
        # list_p_min_pas = np.array([tuple(row) for row in list_p_min_pas], dtype=dt)
        # list_p_min_pas = [list(n) for n in list_p_min_pas.tolist()]
        # list_p_moy_pas = np.array([tuple(row) for row in list_p_moy_pas], dtype=dt)
        # list_p_moy_pas = [list(n) for n in list_p_moy_pas.tolist()]

        list_p_diurne = list_p_diurne.tolist()
        list_p_nocturne = list_p_nocturne.tolist()
        list_p_creuses = list_p_creuses.tolist()
        list_p_pleines = list_p_pleines.tolist()
        list_p_pointes = list_p_pointes.tolist()

        return list_p_max_pas, list_p_min_pas, list_p_moy_pas, list_e_all_hours, list_e_pointes, list_e_nocturne, list_e_pleines, list_e_creuses, list_e_diurne, list_p_pointes, list_p_creuses, list_p_pleines, list_p_nocturne, list_p_diurne

    def store_stat(self, collections_stat=None):
        """
        store in mongodb stat collection
        :param collections_stat: list of string collectections stat name
        :return: 
        """
        self._start_time = time.time()
        my_message = EsMessage()
        # try:
        #     self._con_mg.close()
        # except AttributeError:
        #     pass
        self.connect_db()
        message = None
        erreur = False
        type_message = 'warning'
        if type(collections_stat) == list or collections_stat is None:
            if collections_stat is not None:
                if not sublist(collections_stat, self._collections_stat):
                    erreur = True
                    message = "la collection de stat n'est pas correcte"
                    type_message = 'warning'
                else:
                    self._collections_stat = collections_stat

            if not erreur:
                for collection_stat in self._collections_stat:
                    print('collection stat {} : {}'.format(collection_stat, time.time() - self._start_time))
                    # TODO change _bis
                    if collection_stat == 'stat15m':
                        self._pas = 900
                    elif collection_stat == 'statheure':
                        self._pas = 3600
                    else:
                        # si statjour
                        self._pas = 86400
                    for self._equipement in self._list_equipements:
                        self._dict_datas = {}
                        message, type_message = self.build_stat_mesures(collection_stat)
        self._con_mg.close()
        print('end store: {}'.format(time.time() - self._start_time))
        return my_message.get_message(message, type_message)

    def store_stat_stream(self, collections_stat=None):
        """
        store in mongodb stat collection
        :param collections_stat: list of string collectections stat name
        :return:
        """
        self._start_time = time.time()
        my_message = EsMessage()
        # try:
        #     self._con_mg.close()
        # except AttributeError:
        #     pass
        self.connect_db()
        message = None
        erreur = False
        type_message = 'warning'
        if type(collections_stat) == list or collections_stat is None:
            if collections_stat is not None:
                if not sublist(collections_stat, self._collections_stat):
                    erreur = True
                    message = "la collection de stat n'est pas correcte"
                    type_message = 'warning'
                else:
                    self._collections_stat = collections_stat

            if not erreur:
                for collection_stat in self._collections_stat:
                    print('collection stat {} : {}'.format(collection_stat, time.time() - self._start_time))
                    # TODO change _bis
                    if collection_stat == 'stat15m':
                        self._pas = 900
                    elif collection_stat == 'statheure':
                        self._pas = 3600
                    else:
                        # si statjour
                        self._pas = 86400
                    for self._equipement in self._list_equipements:
                        self._dict_datas = {}
                        message, type_message = self.build_stat_mesures_stream(collection_stat)
        self._con_mg.close()
        print('end store: {}'.format(time.time() - self._start_time))
        return my_message.get_message(message, type_message)

    def get_stat(self, batiment, etage, first_date, second_date, froid, total):
        """
        récupère les statistiques correspondant à la demande du user
        :param froid: 
        :param batiment: 
        :param etage: 
        :param first_date: 
        :param second_date: 
        """
        my_message = EsMessage()
        try:
            self._con_mg.close()
        except AttributeError:
            pass
        self.connect_db()
        # if batiment == 'PV':
        #     slash = ""
        #     expression = ""
        # else:
        slash = "/"
        if froid == 'only':
            expression = 'GF\w'
        else:
            expression = '\w{0,3}'
        if (second_date is None and first_date is not None) or (second_date == first_date):
            # stat à une date précise
            isdate, first_date_str, first_date_obj = validate_date(first_date)
            first_date_str = datetime.datetime.strftime(first_date_obj, '%Y-%m-%d')
            if isdate:
                # si la date est conforme
                self._collection_stat = 'stat15m'
                self._pas = 900
                if batiment is not None and etage is not None:
                    # stat d'un batiment et un étage donné à une date précise
                    cle = batiment + slash + etage + '/' + first_date_str
                elif batiment is not None and etage is None:
                    # stat d'un batiment à une date précise
                    cle = batiment + slash + expression + '/' + first_date_str
                elif batiment is None and etage is None:
                    # stat d'un batiment à une date précise
                    cle = first_date_str
                else:
                    return None
                try:
                    if batiment is None and etage is None:
                        if not total:
                            if froid is None or froid == 'only':
                                self._datas = self._db_mg[self._collection_stat].find({'$and': [
                                    {'_id': {'$regex': cle}}, {'_id': {'$regex': 'GF'}}]})
                            elif froid == 'without':
                                self._datas = self._db_mg[self._collection_stat].find({'$and': [
                                    {'_id': {'$regex': cle}}, {'_id': {'$not': re.compile('GF')}},
                                    {'_id': {'$not': re.compile('PV')}}, {'_id': {'$not': re.compile('BATT')}}]})
                        else:
                            # TODO A DEVELOPPER CONSO - PV
                            self._datas = self._db_mg[self._collection_stat].find({'_id': {'$regex': cle}})

                            pass

                    else:
                        if froid is None or froid == 'only':
                            self._datas = self._db_mg[self._collection_stat].find({'_id': {'$regex': cle}})
                        elif froid == 'without':
                            self._datas = self._db_mg[self._collection_stat].find(
                                {'_id': {'$regex': cle, '$not': re.compile('GF')}})
                except OSError as e:
                    message = 'Error: Datas was not found in database: {}'.format(e)
                    type_message = 'warning'
                    message_format = my_message.get_message(message, type_message)
                    self._con_mg.close()
                    return message_format

        elif second_date is not None and first_date is not None:
            # stat entre 2 dates
            isdate, first_date_str, first_date_obj = validate_date(first_date)
            if isdate:
                isdate, second_date_str, second_date_obj = validate_date(second_date)
                if isdate:
                    # si les 2 dates sont conformes
                    nbjours = second_date_obj - first_date_obj
                    nbjours = nbjours.days
                    if nbjours > 0:
                        if 6 <= nbjours <= 27:
                            self._collection_stat = 'statheure'
                            self._pas = 3600
                        elif nbjours > 27:
                            self._collection_stat = 'statjour'
                            self._pas = 86400
                        else:
                            self._collection_stat = 'stat15m'
                            self._pas = 900
                        isodate_first = first_date_obj.isoformat()
                        second_date_obj = second_date_obj + + datetime.timedelta(days=1)
                        isodate_second = second_date_obj.isoformat()
                        if batiment is not None and etage is not None:
                            # stat d'un batiment et un étage donné entre 2 dates
                            cle = batiment + slash + etage + '/'
                        elif batiment is not None and etage is None:
                            # stat d'un batiment entre 2 dates
                            cle = batiment + slash + expression + '/'
                        elif batiment is None and etage is None:
                            # stat d'un batiment entre 2 dates
                            cle = None
                        else:
                            self._con_mg.close()
                            return
                        if cle is not None:
                            # stat cle + entre 2 dates

                            try:
                                if froid is None or froid == 'only':
                                    self._datas = self._db_mg[self._collection_stat].find(
                                        {'_id': {'$regex': cle}, 'date': {'$gte': isodate_first,
                                                                          '$lt': isodate_second}}).sort('date', 1)
                                elif froid == 'without':
                                    self._datas = self._db_mg[self._collection_stat].find(
                                        {'_id': {'$regex': cle, '$not': re.compile('GF')},
                                         'date': {'$gte': isodate_first,
                                                  '$lt': isodate_second}}).sort('date', 1)
                            except OSError as e:
                                message = 'Error: Datas was not found in database: {}'.format(e)
                                type_message = 'warning'
                                message_format = my_message.get_message(message, type_message)
                                self._con_mg.close()
                                return message_format

                        else:
                            # stat entre 2 dates
                            try:
                                if not total:
                                    if froid is None or froid == 'only':
                                        self._datas = self._db_mg[self._collection_stat].find({'$and': [
                                            {'date': {'$gte': isodate_first, '$lt': isodate_second}},
                                            {'_id': {'$regex': 'GF'}}]}).sort('date', 1)
                                    elif froid == 'without':
                                        self._datas = self._db_mg[self._collection_stat].find({'$and': [
                                            {'date': {'$gte': isodate_first,
                                                      '$lt': isodate_second}}, {'_id': {'$not': re.compile('GF')}}, {'_id': {'$not': re.compile('PV')}}, {'_id': {'$not': re.compile('BATT')}}]}).sort('date', 1)
                                else:
                                    # TODO A DEVELOPPER CONSO - PV
                                    self._datas = self._db_mg[self._collection_stat].find({'date': {'$gte': isodate_first, '$lt': isodate_second}}).sort('date', 1)

                                    pass
                            except OSError as e:
                                message = 'Error: Datas was not found in database: {}'.format(e)
                                type_message = 'warning'
                                message_format = my_message.get_message(message, type_message)
                                self._con_mg.close()
                                return message_format

        self._con_mg.close()
        message = 'OK'
        type_message = 'success'
        message_format = my_message.get_message(message, type_message)

        if etage is None or batiment is None:
            self.sum_data_date(total)
        return message_format

    def transmit_graph_stat(self, date_first, date_second, froid, datas=None):
        """
        stream graph at a determined frequency (time_interval)
        :return: the obj graph and the result message
        """
        pv = False
        graph_min_max_mean = None
        graph_diurne_nocturne = None
        graph_creuse_pleine_pointe = None
        i_color = 0
        self._list_graph = []
        if datas is not None:
            self._datas = datas
        periode = "du {} au {}".format(date_first, date_second) if (date_first != date_second) and \
                                                                   (date_second is not None) else \
            "du {}".format(date_first)
        if self._datas[0]['_id'][:2] == 'PV':
            qualificatif = 'produite'
            pv = True
        else:
            qualificatif = 'consommée'
        type_message = 'warning'
        mode = 'time'
        unit = ""
        message = ""
        my_message = EsMessage()
        l_type_message = []

        if froid == 'only':
            type_conso = 'Climatisation'
        elif froid == 'without':
            type_conso = 'Autres usages'
        elif froid is None:
            type_conso = 'Globale'
        if self._datas:
            for stat in self._list_stat:
                if stat == 'energie_pointes':
                    legend = 'Heures de pointe'
                elif stat == 'energie_creuses':
                    legend = 'Heures creuses'
                elif stat == 'energie_pleines':
                    legend = 'Heures pleines'
                elif stat == 'energie_diurne':
                    legend = 'Energie diurne'
                elif stat == 'energie_nocturne':
                    legend = 'Energie nocturne'
                elif stat == 'p_max':
                    legend = 'Puissance maximale'
                elif stat == 'p_min':
                    legend = 'Puissance minimale'
                elif stat == 'p_mean':
                    legend = 'Puissance moyenne'
                else:
                    legend = stat
                # iteration sur chaque graphe
                xn, xn_utc, xn_iso, xn_iso_utc, xn2, x, yn, y, list_date = [], [], [], [], [], [], [], [], []
                for item in self._datas:
                    # iteration sur chaque date
                    for data in item[stat]:
                        # iteration sur chaque valeur pour le pas
                        iso_to_datetime = dateutil.parser.parse(data[0])

                        # timestamp local time
                        timestamp_s = time.mktime(iso_to_datetime.timetuple())

                        # timestamp et iso date en utc
                        tuple_utc = time.gmtime(timestamp_s)
                        ts_utc = time.mktime(tuple_utc)
                        dt_utc = datetime.datetime.fromtimestamp(ts_utc)
                        iso_utc = dt_utc.isoformat()

                        xn.append(timestamp_s + 3600)  # dates locales
                        xn_utc.append(ts_utc)  # dates utc
                        # iso_utc
                        xn_iso_utc.append(iso_utc)  # date iso utc
                        xn_iso.append(data[0])  # date iso heure local
                        list_date.append(iso_to_datetime)
                        yn.append(data[1])
                # conversion Kw et Kwh hors PV
                if not pv:
                    yn = np.array(yn)
                    yn = yn / 1000
                    yn = yn.tolist()
                time_dt_s = EsQuantity(self._pas, 'second')
                time_dt_h = time_dt_s.to('h')
                xlabel = 'Temps (pas de ' + str(time_dt_h.value) + ' heure(s))'
                unitx = 'Date/heure'
                if stat in ['p_max', 'p_min', 'p_mean']:
                    ylabel = 'kW'
                    type_graph = "lines"
                    name = "{} - puissance {} {}".format(type_conso, qualificatif, periode)
                    color = self._color[i_color]
                    i_color += 1
                    if graph_min_max_mean is None:
                        graph_min_max_mean = {
                            'name': name,
                            'ylabel': ylabel,
                            'xlabel': xlabel,
                            'mode': mode,
                            'data': [
                                {
                                    'type': type_graph,
                                    # 'time_depart': xn[0],
                                    'time_dt': time_dt_s.value,
                                    'Y': {
                                        "dataY": yn,
                                        "unit": unit
                                    },
                                    'X': {
                                        "dataX": xn,
                                        "unit": unitx
                                    },

                                    'legend': legend,
                                    'color': color,
                                    'points': True
                                }
                            ]
                        }
                    else:
                        gph = {
                            'type': type_graph,
                            # 'time_depart': xn[0],
                            'time_dt': time_dt_s.value,
                            'Y': {
                                "dataY": yn,
                                "unit": unit
                            },
                            'X': {
                                "dataX": xn,
                                "unit": unitx
                            },
                            'legend': legend,
                            'color': color,
                            'points': True
                        }
                        graph_min_max_mean['data'].append(gph)
                elif stat in ['energie_diurne', 'energie_nocturne']:
                    color = self._color[i_color]
                    i_color += 1
                    type_graph = "bars"
                    ylabel = 'kWh'
                    name = "{} - énergie {} {}".format(type_conso, qualificatif, periode)
                    unitx = 'Date/heure'

                    if graph_diurne_nocturne is None:
                        graph_diurne_nocturne = {
                            'name': name,
                            'ylabel': ylabel,
                            'xlabel': xlabel,
                            'mode': mode,
                            'data': [
                                {
                                    'type': type_graph,
                                    # 'time_depart': xn[0],
                                    'time_dt': time_dt_s.value,
                                    'Y': {
                                        "dataY": yn,
                                        "unit": unit
                                    },
                                    'X': {
                                        "dataX": xn,
                                        "unit": unitx
                                    },

                                    'legend': legend,
                                    'color': color,
                                    'stack': True
                                }
                            ]
                        }
                    else:
                        gph = {
                            'type': type_graph,
                            # 'time_depart': xn[0],
                            'time_dt': time_dt_s.value,
                            'Y': {
                                "dataY": yn,
                                "unit": unit
                            },
                            'X': {
                                "dataX": xn,
                                "unit": unitx
                            },
                            'legend': legend,
                            'color': color,
                            'stack': True
                        }
                        graph_diurne_nocturne['data'].append(gph)
                if stat in ['energie_pointes', 'energie_creuses', 'energie_pleines']:
                    type_graph = "bars"
                    ylabel = 'kWh'
                    name = "{} - énergie {} {}".format(type_conso, qualificatif, periode)
                    unitx = 'Date/heure'

                    color = self._color[i_color]
                    i_color += 1
                    if graph_creuse_pleine_pointe is None:
                        graph_creuse_pleine_pointe = {
                            'name': name,
                            'ylabel': ylabel,
                            'xlabel': xlabel,
                            'mode': mode,
                            'data': [
                                {
                                    'type': type_graph,
                                    # 'time_depart': xn[0],
                                    'time_dt': time_dt_s.value,
                                    'Y': {
                                        "dataY": yn,
                                        "unit": unit
                                    },
                                    'X': {
                                        "dataX": xn,
                                        "unit": unitx
                                    },
                                    'legend': legend,
                                    'color': color,
                                    'stack': True
                                }
                            ]
                        }
                    else:
                        gph = {
                            'type': type_graph,
                            # 'time_depart': xn[0],
                            'time_dt': time_dt_s.value,
                            'Y': {
                                "dataY": yn,
                                "unit": unit
                            },
                            'X': {
                                "dataX": xn,
                                "unit": unitx
                            },
                            'legend': legend,
                            'color': color,
                            'stack': True
                        }
                        graph_creuse_pleine_pointe['data'].append(gph)
                l_type_message.append('success')
                if type(self._datas) is not list:
                    self._datas.rewind()
            if len(l_type_message) > 1:
                type_message = 'success'
                for mes in l_type_message:
                    if mes != 'success':
                        type_message = 'warning'
        else:
            message = 'Pas de dataseries à cette date'
            type_message = 'warning'
        self._list_graph = [graph_min_max_mean, graph_diurne_nocturne, graph_creuse_pleine_pointe]
        message_format = my_message.get_message(message, type_message)
        return {'obj_graph': self._list_graph, 'message': message_format}

    def stat_to_front(self, bat, etage, first_date, second_date, froid, total):
        """
        :param total: 
        :param bat: 
        :param etage: 
        :param first_date: 
        :param second_date: 
        :param froid: 3 values (None, 'only', 'without') 
        :return: 
        """
        self.get_stat(bat, etage, first_date, second_date, froid, total)
        return self.transmit_graph_stat(first_date, second_date, froid)

    def sum_data_date(self, all_site):
        """
        somme les datas stat pour un même batiment
        """
        list_datas_sum = []
        dict_sum = {}
        index_row = 0
        date_previous = None
        for data in self._datas:
            date_current = data['date']
            split_id = data['_id'].split('/')
            batiment = split_id[0]
            if (batiment == 'PV') and all_site:
                substract = True
            else:
                substract = False
            # if not the first row/date
            if index_row > 0:
                # if current date is the same than previous date
                if date_current == date_previous:
                    # sum the value tab
                    for field in ['p_max', 'p_min', 'p_mean', 'energie_pleines', 'energie_creuses', 'energie_pointes',
                                  'energie_diurne', 'energie_nocturne', 'energie_heures']:
                        index_tab_value = 0
                        for item in data[field]:
                            if substract:
                                dict_sum[field][index_tab_value][1] -= item[1]
                            else:
                                dict_sum[field][index_tab_value][1] += item[1]
                            index_tab_value += 1
                # if current date is different from previous date
                else:
                    list_datas_sum.append(dict_sum)
                    dict_sum = data
            else:
                dict_sum = data
            index_row += 1
            date_previous = date_current
        list_datas_sum.append(dict_sum)
        self._datas = list_datas_sum

    def stream_calculate_stat(self, coll_stat, coll_mesures, clck):
        """

        :param date_current: 
        :param coll_stat: list - ex: ['stat15m']
        :param coll_mesures: string - ex: 'mesures'
        """
        if 'stat15m' in coll_stat:
            self._clk15m = clck
        elif 'statheure' in coll_stat:
            self._clkheure = clck
        self._date_encours = datetime.datetime.now()
        self.set_collection_mesures(coll_mesures)
        self.store_stat_stream(collections_stat=coll_stat)
        pass

    def add_inexistant_values(self, p_list, collection_statistique='stat15m'):
        """
        Rajoute les valeurs inexistantes de la journéé jusqu'à l'heure final
        :param collection_statistique:
        :param p_list:
        :return:
        """
        p_list_final = []
        if collection_statistique == 'stat15m':
            s_final = time_to_seconds(self._clk15m)
        elif collection_statistique == 'statheure':
            s_final = time_to_seconds(self._clkheure)
        else:
            s_final = time_to_seconds(self._clk15m)
        t = datetime.datetime.combine(datetime.datetime.now().date(), datetime.time(0,0,0))
        i = 0
        j = 0
        while i <= s_final:
            try:
                if p_list[j][0] == t:
                    p_list_final.append(p_list[j])
                    j += 1
                else:
                    p_list_final.append([t, None])
            except IndexError:
                    p_list_final.append([t, None])

            t = t + datetime.timedelta(seconds=1)
            i += 1
        return p_list_final


def time_to_seconds(timestr):
    x = time.strptime(timestr.split(',')[0], '%H:%M:%S')
    seconds = int(datetime.timedelta(hours=x.tm_hour, minutes=x.tm_min, seconds=x.tm_sec).total_seconds())
    return seconds


def second_to_time(seconds):
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h == 24:
        h = 0
    return datetime.time(h, m, s)


def get_energy(list_datas, pas_datas):
    """
    return energy in limits for a dt
    :rtype: object
    :param list_datas: 
    :param pas: 
    """
    x = np.array(list_datas, dtype=np.float64)
    x = x[~np.isnan(x)]
    energy = np.sum(x) * pas_datas
    return energy


def sublist(lst1: list, lst2: list) -> bool:
    ls1 = [element for element in lst1 if element in lst2]
    ls2 = [element for element in lst2 if element in lst1]
    return ls1 == ls2


if __name__ == '__main__':
    stat_sup = StatsSupervision()
    # stat_sup(date='2017-01-02')
    stat_sup()
    dte = datetime.datetime.now()
    clock_now = dte.strftime('%H:%M:%S')
    stat_sup._collection_mesures = 'mesures_stream'
    stat_sup._equipement = 'ADMI/RDC'
    stat_sup.get_day_series(date_deb='2018-03-23', date_fin='2018-03-23')


    # date_j = '2017-02-28'
    # stat_sup.store_stat(['statheure'])
    # o_graph = stat_sup.stat_to_front('ADMI', None, '2017-02-01', '2017-02-10', 'without',True)
    # stat_sup.stream_calculate_stat(['stat15m'], 'mesures_stream', '11:05:23')
    pass
