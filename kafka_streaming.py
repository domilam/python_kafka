from kafka import KafkaProducer
import threading
import json
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (WriteRowsEvent)
import datetime
from clock import ClockAloe
from dateutil import tz

tags_tables = {'ADMI/RDC': 'supervisiondb_jlomyap3azgkj1h2a05s0000070s4v',
               'ADMI/ET1': 'supervisiondb_jlomyap3azgkj54ycrq0000030s4v'}


class Producer(threading.Thread, ClockAloe):
    daemon = True

    def __init__(self, tm_interval=1, port='localhost:9092', topic='edfstream',
                 vtscada_dbs='vtscadasupervision'):
        super().__init__()
        self.interval = tm_interval
        self.port = port
        self.topic = topic
        self._pas_vt = 1 / 60 / 60
        self._tag_vt = None
        self._value_vt = None
        self._date_vt = None
        self._date_prec_vt = None
        self._puissance_m_vt = {}
        self._date_now_vt = datetime.datetime.strftime(datetime.date.today(), '%Y-%m-%d')
        self._stream = BinLogStreamReader(
            connection_settings={
                "host": "127.0.0.1",
                "port": 3306,
                "user": "root",
                "passwd": ""},
            server_id=1,
            blocking=True,
            resume_stream=True,
            only_events=[WriteRowsEvent],
            only_schemas=[vtscada_dbs])

        t = datetime.datetime.now().time()
        # init clock with current date system
        ClockAloe.__init__(self, t.hour, t.minute, t.second)
        self.producer = KafkaProducer(bootstrap_servers=self.port,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def producer_stream(self):
        """
        Stream data from mysql to kafka
        """

        # browse the stream event
        for binlogevent in self._stream:
            for row in binlogevent.rows:
                event = {"schema": binlogevent.schema,
                         "table": binlogevent.table,
                         "type": type(binlogevent).__name__,
                         "row": row
                         }

            # for each tag stream to kafka
            for tag in tags_tables:
                if 'values' in event['row'].keys() and event['table'] == tags_tables[tag]:
                    if 'Timestamp' in event['row']['values'].keys() and 'Value' in event['row']['values'].keys():
                        local_datetime = convert_to_local(event['row']['values']['Timestamp'])
                        ts = local_datetime.timestamp()
                        self._date_vt = datetime.datetime.fromtimestamp(ts)
                        time_encours = self._date_vt.time()
                        self.set_clock(time_encours.hour, time_encours.minute, time_encours.second)
                        self._tag_vt = tag
                        self._value_vt = event['row']['values']['Value']
                        # print('{} - Date: {} - Value: {}'.format(self._tag_vt, local_datetime,
                        #                                         event['row']['values']['Value']))
                        # when date change initialize puissance_m
                        if self._date_prec_vt:
                            if self._date_prec_vt != self._date_vt:
                                self._date_prec_vt = self._date_vt
                                self._puissance_m_vt[tag] = []

                        # stream to kafka
                        # Asynchronous by default

                        self.producer.send(self.topic, (ts, event['row']['values']['Value'], tag))
                        print((datetime.datetime.fromtimestamp(ts), ts, event['row']['values']['Value'], tag))
                        # time.sleep(self.interval)


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

if __name__ == '__main__':
    producer = Producer()
    producer.producer_stream()
