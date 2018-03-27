from unittest import TestCase
from utils_supervision import *
from stats_supervision import *
import datetime


class TestUtilsSp(TestCase):
    def test_time_to_seconds_1(self):
        self.assertEqual(time_to_seconds('18:00:00'), 64800)

    def test_time_to_seconds_2(self):
        self.assertEqual(time_to_seconds('18:00:00,002'), 64800)

    def test_time_to_second_3(self):
        self.assertEqual(time_to_seconds('00:00:00'), 0)

    def test_time_to_second_4(self):
        self.assertEqual(time_to_seconds('23:59:59'), 86399)

    def test_time_to_second_5(self):
        self.assertEqual(time_to_seconds('00:00:02'), 2)

    def test_sublist(self):
        l1 = ['stat15m']
        l2 = ['stat15m', ['statheure'], ['statjour']]
        self.assertEqual(sublist(l1, l2), True)

    def test_get_stat1(self):
        stat = StatsSupervision()
        stat(company='test', db_auth_name='aloe_auth_test')
        stat.get_stat('ADMI', None, '2017-02-01', None, 'without', False)
        self.assertEqual(len(stat.get_datas()), 1)

    def test_get_stat2(self):
        stat = StatsSupervision()
        stat(company='test', db_auth_name='aloe_auth_test')
        stat.get_stat('ADMI', 'ET1', '2017-02-01', None, 'without', False)
        self.assertEqual(stat.get_datas().count(), 1)

    def test_get_stat3(self):
        stat = StatsSupervision()
        stat(company='test', db_auth_name='aloe_auth_test')
        stat.get_stat(None, None, '2017-02-01', None, 'without', False)
        self.assertEqual(len(stat.get_datas()), 1)

    def test_get_stat4(self):
        stat = StatsSupervision()
        stat(company='test', db_auth_name='aloe_auth_test')
        stat.get_stat('ADMI', None, '2017-02-01', '2017-02-01', 'without', False)
        self.assertEqual(len(stat.get_datas()), 1)

    def test_get_stat5(self):
        stat = StatsSupervision()
        stat(company='test', db_auth_name='aloe_auth_test')
        stat.get_stat('ADMI', 'ET1', '2017-02-01', '2017-02-03', 'without', False)
        self.assertEqual(stat.get_datas().count(), 3)

    def test_get_stat6(self):
        stat = StatsSupervision()
        stat(company='test', db_auth_name='aloe_auth_test')
        stat.get_stat('ADMI', None, '2017-02-01', '2017-02-03', 'without', False)
        self.assertEqual(len(stat.get_datas()), 3)

    def test_get_stat7(self):
        stat = StatsSupervision()
        stat(company='test', db_auth_name='aloe_auth_test')
        stat.get_stat(None, None, '2017-02-01', '2017-02-02', 'without', False)
        self.assertEqual(len(stat.get_datas()), 2)

    # test période < 6 jours ==> stat 15 minutes
    def test_transmit_graph_stat1(self):
        stat = StatsSupervision()
        stat(company='test', db_auth_name='aloe_auth_test')
        stat.get_stat('ADMI', 'ET1', '2017-02-01', '2017-02-06', 'without', False)
        obj_graph = stat.transmit_graph_stat('2017-02-01', '2017-02-06', 'without')
        self.assertNotEqual(None, obj_graph['obj_graph'])
        self.assertEqual('success', obj_graph['message']['type'])
        self.assertEqual(3, len(obj_graph['obj_graph']))
        self.assertEqual(obj_graph['obj_graph'][0]['data'][0]['X']['dataX'][0], 1485926100.0)
        self.assertEqual(len(obj_graph['obj_graph'][0]['data'][0]['Y']['dataY']), 576)

    def test_transmit_graph_stat6(self):
        stat = StatsSupervision()
        stat(company='test', db_auth_name='aloe_auth_test')
        stat.get_stat('PV', None, '2017-02-01', '2017-02-06', 'without', False)
        obj_graph = stat.transmit_graph_stat('2017-02-01', '2017-02-06', 'without')
        self.assertNotEqual(None, obj_graph['obj_graph'])
        self.assertEqual('success', obj_graph['message']['type'])
        self.assertEqual(3, len(obj_graph['obj_graph']))
        self.assertEqual(obj_graph['obj_graph'][0]['data'][0]['X']['dataX'][0], 1485926100.0)
        self.assertEqual(len(obj_graph['obj_graph'][0]['data'][0]['Y']['dataY']), 576)

    # test 1 jour ==> stat 15 minutes
    def test_transmit_graph_stat2(self):
        stat = StatsSupervision()
        stat(company='test', db_auth_name='aloe_auth_test')
        stat.get_stat('ADMI', 'ET1', '2017-02-01', '2017-02-01', 'without', False)
        obj_graph = stat.transmit_graph_stat('2017-02-01', '2017-02-01', 'without')
        self.assertNotEqual(None, obj_graph['obj_graph'])
        self.assertEqual('success', obj_graph['message']['type'])
        self.assertEqual(3, len(obj_graph['obj_graph']))
        self.assertEqual(obj_graph['obj_graph'][0]['data'][0]['X']['dataX'][0], 1485926100.0)
        self.assertEqual(len(obj_graph['obj_graph'][0]['data'][0]['Y']['dataY']), 96)

    def test_transmit_graph_stat5(self):
        stat = StatsSupervision()
        stat(company='test', db_auth_name='aloe_auth_test')
        stat.get_stat('PV', None, '2017-02-01', '2017-02-01','without', False)
        obj_graph = stat.transmit_graph_stat('2017-02-01', '2017-02-01', 'without')
        self.assertNotEqual(None, obj_graph['obj_graph'])
        self.assertEqual('success', obj_graph['message']['type'])
        self.assertEqual(3, len(obj_graph['obj_graph']))
        self.assertEqual(obj_graph['obj_graph'][0]['data'][0]['X']['dataX'][0], 1485926100.0)
        self.assertEqual(len(obj_graph['obj_graph'][0]['data'][0]['Y']['dataY']), 96)

    # test stat 6=< jours <=27  ==> statheure
    def test_transmit_graph_stat3(self):
        stat = StatsSupervision()
        stat(company='test', db_auth_name='aloe_auth_test')
        stat.get_stat('ADMI', 'ET1', '2017-02-01', '2017-02-07', 'without', False)
        obj_graph = stat.transmit_graph_stat('2017-02-01', '2017-02-07', 'without')
        self.assertNotEqual(None, obj_graph['obj_graph'])
        self.assertEqual('success', obj_graph['message']['type'])
        self.assertEqual(3, len(obj_graph['obj_graph']))
        self.assertEqual(obj_graph['obj_graph'][0]['data'][0]['X']['dataX'][0], 1485928800.0)
        self.assertEqual(len(obj_graph['obj_graph'][0]['data'][0]['Y']['dataY']), 168)

    # test stat jours >=28  ==> statjour
    def test_transmit_graph_stat4(self):
        stat = StatsSupervision()
        stat(company='test', db_auth_name='aloe_auth_test')
        stat.get_stat('ADMI', None, '2017-02-01', '2017-02-28','without', False)
        obj_graph = stat.transmit_graph_stat('2017-02-01', '2017-02-28', 'without')
        self.assertNotEqual(None, obj_graph['obj_graph'])
        self.assertEqual('success', obj_graph['message']['type'])
        self.assertEqual(3, len(obj_graph['obj_graph']))
        self.assertEqual(obj_graph['obj_graph'][0]['data'][0]['X']['dataX'][0], 1485928800.0)
        self.assertEqual(len(obj_graph['obj_graph'][0]['data'][0]['Y']['dataY']), 168)
