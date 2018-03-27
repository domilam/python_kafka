from unittest import TestCase
from utils_supervision import *
from stats_supervision import *
import datetime


class TestUtilsSp(TestCase):
    def test_build_stat_mesures1(self):
        stat = StatsSupervision()
        # dte = datetime.datetime(2017,2,1)
        stat(company='test', db_auth_name='aloe_auth_test', date='2017-02-01', pas=900)
        stat.set_list_equipements = ['ADMI/RDC']
        stat.build_stat_mesures(equipement="ADMI/RDC")
        dict_datas = stat.get_dict_datas()
        self.assertEqual(len(dict_datas), 1)
        self.assertEqual('ADMI/RDC/2017-02-01'in dict_datas.keys(), True)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['p_max']), 96)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['p_min']), 96)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['p_mean']), 96)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['energie_heures']), 96)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['energie_diurne']), 48)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['energie_nocturne']), 48)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['energie_pleines']), 24)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['energie_creuses']), 40)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['energie_pointes']), 32)

    def test_build_stat_mesures2(self):
        stat = StatsSupervision()
        # dte = datetime.datetime(2017,2,1)
        stat(company='test', db_auth_name='aloe_auth_test', date='2017-02-01', pas=3600)
        stat.set_list_equipements = ['ADMI/RDC']
        stat.build_stat_mesures(equipement="ADMI/RDC")
        dict_datas = stat.get_dict_datas()
        dict_test_datas = stat.get_test_datas()

        self.assertEqual(len(dict_datas), 1)
        self.assertEqual('ADMI/RDC/2017-02-01'in dict_datas.keys(), True)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['p_max']), 24)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['p_min']), 24)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['p_mean']), 24)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['energie_heures']), 24)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['energie_diurne']), 12)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['energie_nocturne']), 12)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['energie_pleines']), 6)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['energie_creuses']), 10)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['energie_pointes']), 8)

        # test des bornes de pleines
        self.assertEqual(dict_datas['ADMI/RDC/2017-02-01']['p_max'][0][0], '2017-02-01T01:00:00')
        self.assertEqual(dict_datas['ADMI/RDC/2017-02-01']['p_max'][23][0], '2017-02-01T23:59:59')
        self.assertEqual(dict_datas['ADMI/RDC/2017-02-01']['p_min'][0][0], '2017-02-01T01:00:00')
        self.assertEqual(dict_datas['ADMI/RDC/2017-02-01']['p_min'][23][0], '2017-02-01T23:59:59')
        self.assertEqual(dict_datas['ADMI/RDC/2017-02-01']['p_mean'][0][0], '2017-02-01T01:00:00')
        self.assertEqual(dict_datas['ADMI/RDC/2017-02-01']['p_mean'][23][0], '2017-02-01T23:59:59')

        self.assertEqual(dict_datas['ADMI/RDC/2017-02-01']['energie_pleines'][0][0], '2017-02-01T07:00:00')
        self.assertEqual(dict_datas['ADMI/RDC/2017-02-01']['energie_pleines'][1][0], '2017-02-01T13:00:00')
        # self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_pleines'][18000][0], datetime.datetime(2017, 2, 1, 20, 0, 0))
        # self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_pleines'][21599][0], datetime.datetime(2017, 2, 1, 20, 59, 59))
        #
        # # test des bornes de creuses
        # self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_creuses'][0][0], datetime.datetime(2017, 2, 1, 0, 0, 0))
        # self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_creuses'][25199][0], datetime.datetime(2017, 2, 1, 6, 59, 59))
        # self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_creuses'][25200][0], datetime.datetime(2017, 2, 1, 21, 0, 0))
        # self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_creuses'][35999][0], datetime.datetime(2017, 2, 1, 23, 59, 59))
        #
        # # test des bornes de pointes
        # self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_pointes'][0][0], datetime.datetime(2017, 2, 1, 8, 0, 0))
        # self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_pointes'][17999][0], datetime.datetime(2017, 2, 1, 12, 59, 59))
        # self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_pointes'][18000][0], datetime.datetime(2017, 2, 1, 17, 0, 0))
        # self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_pointes'][28799][0], datetime.datetime(2017, 2, 1, 19, 59, 59))
        #
        # # test des bornes de nocturne
        # self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_nocturne'][0][0], datetime.datetime(2017, 2, 1, 0, 0, 0))
        # self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_nocturne'][21599][0], datetime.datetime(2017, 2, 1, 5, 59, 59))
        # self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_nocturne'][21600][0], datetime.datetime(2017, 2, 1, 18, 0, 0))
        # self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_nocturne'][43199][0], datetime.datetime(2017, 2, 1, 23, 59, 59))
        #
        # # test des bornes de diurne
        # self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_diurne'][0][0], datetime.datetime(2017, 2, 1, 6, 0, 0))
        # self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_diurne'][43199][0], datetime.datetime(2017, 2, 1, 17, 59, 59))

    # test pour les stats jour
    def test_build_stat_mesures3(self):
        stat = StatsSupervision()
        # dte = datetime.datetime(2017,2,1)
        stat(company='test', db_auth_name='aloe_auth_test', date='2017-02-01', pas=86400)
        stat.set_list_equipements = ['ADMI/RDC']
        stat.build_stat_mesures(equipement="ADMI/RDC")
        dict_datas = stat.get_dict_datas()
        dict_test_datas = stat.get_test_datas()
        self.assertEqual(len(dict_datas), 1)
        self.assertEqual('ADMI/RDC/2017-02-01'in dict_datas.keys(), True)

        # test des nombres de résultats
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['p_max']), 1)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['p_min']), 1)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['p_mean']), 1)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['energie_heures']), 1)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['energie_diurne']), 1)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['energie_nocturne']), 1)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['energie_pleines']), 1)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['energie_creuses']), 1)
        self.assertEqual(len(dict_datas['ADMI/RDC/2017-02-01']['energie_pointes']), 1)

        # test des bornes de pleines
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_pleines'][0][0], datetime.datetime(2017, 2, 1, 7, 0, 0))
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_pleines'][3599][0], datetime.datetime(2017, 2, 1, 7, 59, 59))
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_pleines'][3600][0], datetime.datetime(2017, 2, 1, 13, 0, 0))
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_pleines'][17999][0], datetime.datetime(2017, 2, 1, 16, 59, 59))
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_pleines'][18000][0], datetime.datetime(2017, 2, 1, 20, 0, 0))
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_pleines'][21599][0], datetime.datetime(2017, 2, 1, 20, 59, 59))

        # test des bornes de creuses
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_creuses'][0][0], datetime.datetime(2017, 2, 1, 0, 0, 0))
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_creuses'][25199][0], datetime.datetime(2017, 2, 1, 6, 59, 59))
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_creuses'][25200][0], datetime.datetime(2017, 2, 1, 21, 0, 0))
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_creuses'][35999][0], datetime.datetime(2017, 2, 1, 23, 59, 59))

        # test des bornes de pointes
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_pointes'][0][0], datetime.datetime(2017, 2, 1, 8, 0, 0))
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_pointes'][17999][0], datetime.datetime(2017, 2, 1, 12, 59, 59))
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_pointes'][18000][0], datetime.datetime(2017, 2, 1, 17, 0, 0))
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_pointes'][28799][0], datetime.datetime(2017, 2, 1, 19, 59, 59))

        # test des bornes de nocturne
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_nocturne'][0][0], datetime.datetime(2017, 2, 1, 0, 0, 0))
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_nocturne'][21599][0], datetime.datetime(2017, 2, 1, 5, 59, 59))
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_nocturne'][21600][0], datetime.datetime(2017, 2, 1, 18, 0, 0))
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_nocturne'][43199][0], datetime.datetime(2017, 2, 1, 23, 59, 59))

        # test des bornes de diurne
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_diurne'][0][0], datetime.datetime(2017, 2, 1, 6, 0, 0))
        self.assertEqual(dict_test_datas['ADMI/RDC/2017-02-01']['p_diurne'][43199][0], datetime.datetime(2017, 2, 1, 17, 59, 59))
