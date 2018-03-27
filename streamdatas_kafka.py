from flask import Flask, jsonify
from collections import deque
from dateutil import tz
from kafka import KafkaConsumer
import datetime
from clock import ClockAloe
from mypymongo import MyPyMongo
import random
from threading import Thread
import time
import json


# TODO Add in tags keys, name of data: ex: puissance, frequence, tension, etc...
tags_tables = {'ADMI/RDC': 'supervisiondb_jlomyap3azgkj1h2a05s0000070s4v', 'ADMI/ET1': 'supervisiondb_jlomyap3azgkj54ycrq0000030s4v'}
queues = {}
queues_fake = {}
values = deque(maxlen=1000)


class VtscadaRealTime(ClockAloe):
    """
    This class is used to stream data added in a list of mysql vtscada databases
    When Write event is detected the datas are saved to mongodb and are sent to the front side to be visualized
    db_mg: pymongo database Object
    coll_mesure : the name of the collection where the datas streamed will be saved: String
    vtscada_dbs: the schema of the mysql database of vtscada: String
    """

    def __init__(self, company='EDF', db_auth_name='aloe_auth', coll_mesure='mesures'):
        self._db_auth_name = db_auth_name
        self._company = company
        self._con, self._db = self.connect_db()

        t = datetime.datetime.now().time()
        # init clock with current date system
        ClockAloe.__init__(self, t.hour, t.minute, t.second)

        # create array of settings.queues equal to the number of tags
        for tag in tags_tables.keys():
            queues['queue_{}'.format(tag)] = deque(maxlen=5000)

        self._coll_mesure_vt = coll_mesure
        self._pas_vt = 1 / 60 / 60
        self._tag_vt = None
        self._value_vt = None
        self._date_vt = None
        self._date_prec_vt = None
        self._puissance_m_vt = {}
        self._date_now_vt = datetime.datetime.strftime(datetime.date.today(), '%Y-%m-%d')

         # initialize dictionary with exist puissance mesures from database
        for tag in tags_tables:
            self._mesure_vt = self._db[self._coll_mesure_vt].find_one({'_id': tag + '/' + self._date_now_vt})
            if self._mesure_vt:
                self._puissance_m_vt[tag] = self._mesure_vt['puissance_m']
            else:
                self._puissance_m_vt[tag] = []

    def consumer_stream(self, save=False):
        KafkaConsumer()
        consumer_data = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='latest',  enable_auto_commit=True)
        consumer_data.subscribe(['edfstream'])
        for message in consumer_data:
            datajson = json.loads(message.value.decode('utf-8'))
            if isinstance(datajson, list):
                timestamp = datajson[0]
                self._date_vt = datetime.datetime.fromtimestamp(timestamp)
                self._tag_vt = datajson[2]
                self._value_vt = datajson[1]
                time_encours = self._date_vt.time()
                self.set_clock(time_encours.hour, time_encours.minute, time_encours.second)
                values.append((timestamp, self._value_vt, self._tag_vt))
                # print((timestamp, self._date_vt, self._value_vt,  self._tag_vt))
                # time.sleep(1)

                # save to mongodb
                if save:
                    self.save_to_db()
                # # add to queue
                queue_tag = 'queue_{}'.format(self._tag_vt)
                if queue_tag in queues.keys():
                    queues[queue_tag].append((timestamp, self._value_vt, self._tag_vt))

    def save_to_db(self):
        """
        save data from stream in mongodb database
        """
        cle_mesure = '{}/{}'.format(self._tag_vt, datetime.datetime.strftime(self._date_vt, '%Y-%m-%d'))
        iso_date = self._date_vt.date().isoformat()
        self._puissance_m_vt[self._tag_vt].append([self._date_vt, self._value_vt])
        document = {'_id': cle_mesure, 'puissance_m': self._puissance_m_vt[self._tag_vt], 'pas': self._pas_vt, 'unit_pas': 'hour', 'date': iso_date}

        self._db[self._coll_mesure_vt].update_one({'_id': cle_mesure}, {'$set': document}, upsert=True)

    def run_streaming(self):
        """
        run the streaming from vtscada to mongodb via mysql
        """
        process = Thread(target=self.consumer_stream)
        process.daemon = True
        process.start()

    def connect_db(self):
        mymongo = MyPyMongo()
        con_auth, db_auth = mymongo(self._db_auth_name)
        # print(db_auth)
        result = db_auth['companies'].find_one({'company': self._company})
        self._con_mg, self._db_mg = mymongo(self._company + '_ALOE_' + 'DB_' + str(result['_id']))
        return self._con_mg, self._db_mg


def init_queues_fake():
    for tag in tags_tables.keys():
        queues_fake['queue_{}'.format(tag)] = deque(maxlen=5000)


def generate_queue_fake():
    for tag in tags_tables.keys():
        dt = datetime.datetime.now()
        ts = time.mktime(dt.timetuple())
        value = round(random.uniform(5000, 10000), 2)
        queues_fake['queue_{}'.format(tag)].append((ts, value, tag))


def values_to_front(reel):
    item = {}
    i = 0
    if reel:
        queues_to_take = queues
    else:
        init_queues_fake()
        queues_to_take = queues_fake
    while True:
        if not reel:
            generate_queue_fake()
        for q in queues_to_take:
            spl_q = q.split('_')
            if q:
                if queues_to_take[q]:
                    item[spl_q[1]] = queues_to_take[q].pop()
                    # print(item[spl_q[1]])
                else:
                    item[spl_q[1]] = None
            else:
                item[spl_q[1]] = None
            if i == len(tags_tables) - 1:
                i = 0
                time.sleep(1)
                yield item
            i += 1


def convert_to_local(utc_date):
    """
    convert utc date to local date
    :param utc_date:
    :return: local date
    """
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()
    utc_date = utc_date.replace(tzinfo=from_zone)
    local = utc_date.astimezone(to_zone)
    return local


def principal():

    vts_rt = VtscadaRealTime(coll_mesure='mesures_stream')
    # process = Thread(target=vts_rt.consumer_stream)
    # process.daemon = True
    # process.start()
    vts_rt.consumer_stream(True)

if __name__ == "__main__":
    principal()
