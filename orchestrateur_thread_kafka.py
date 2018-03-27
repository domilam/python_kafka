import time
import datetime
from threading import Thread, Event
from streamdatas_kafka import VtscadaRealTime
from stats_supervision import StatsSupervision



# class Orchestrator(VtscadaRealTime, StatsSupervision):
class Orchestrator(VtscadaRealTime):
    def __init__(self, facteur_sim):
        self._fact = facteur_sim
        # StatsSupervision.__init__(self)
        # StatsSupervision.__call__(self)
        VtscadaRealTime.__init__(self, coll_mesure='mesures_stream')
        self.run_streaming()

    def get_fact(self):
        return self._fact

    def start_orchestrator(self):
        # t = datetime.datetime.now().time()
        str_time = self.get_clock()
        t = datetime.datetime.strptime(str_time, '%H:%M:%S').time()
        s = (t.hour * 3600 + t.minute * 60 + t.second)

        print('start: {}'.format(s))
        stat15m_state = False
        statheure_state = False
        while True:
            if stat15m_state:
                if not process_quart_heure.is_alive():
                    stat15m_state = False
                    print('end stat15m: {}'.format(s))
            if statheure_state:
                if not process_heure.is_alive():
                    statheure_state = False
                    print('end statheure: {}'.format(s))

            time.sleep(1)
            str_time = self.get_clock()
            t = datetime.datetime.strptime(str_time, '%H:%M:%S').time()
            s = (t.hour * 3600 + t.minute * 60 + t.second)
            pas_quart_heure = 900 * self._fact
            pas_heure = 3600 * self._fact
            if s % pas_quart_heure == 0:
                print('exec stat15m: {}'.format(s))
                self.set_clock_15m(t.hour, t.minute, t.second)
                stop_process_qh = Event()
                process_quart_heure = Thread(target=stat15m, args=(self, stop_process_qh))
                # process_quart_heure.daemon = True
                process_quart_heure.start()
                stat15m_state = True
            if s % pas_heure == 0:
                print('exec statheure: {}'.format(s))
                self.set_clock_heure(t.hour, t.minute, t.second)
                stop_process_h = Event()
                process_heure = Thread(target=statheure, args=(self, stop_process_h))
                # process_heure.daemon = True
                process_heure.start()
                statheure_state = True


def str_time_to_datetime(clck):
    string_time = clck
    t = datetime.datetime.strptime(string_time, '%H:%M:%S').time()
    dte = datetime.datetime.combine(datetime.date.today(), t)
    return dte


def stat15m(obj_orchestrator, stop_event):
    clk = obj_orchestrator.get_clock_15m()
    # TODO UNCOMMENT stream_calculate_stat
    # obj_orchestrator.stream_calculate_stat(['stat15m'], 'mesures_stream', clk)

    # TODO test obj stat par process
    obj_stat = StatsSupervision()
    obj_stat()
    obj_stat.stream_calculate_stat(['stat15m'], 'mesures_stream', clk)
    while not stop_event.is_set():
        stop_event.wait(1 * obj_orchestrator.get_fact())


def statheure(obj_orchestrator, stop_event):
    clk = obj_orchestrator.get_clock_heure()
    # TODO UNCOMMENT stream_calculate_stat
    # obj_orchestrator.stream_calculate_stat(['statheure'], 'mesures_stream', clk)

    # TODO test obj stat par process
    obj_stat = StatsSupervision()
    obj_stat()
    obj_stat.stream_calculate_stat(['statheure'], 'mesures_stream', clk)

    while not stop_event.is_set():
        stop_event.wait(1 * obj_orchestrator.get_fact())


def main():
    orchestrator = Orchestrator(1)
    orchestrator.start_orchestrator()

if __name__ == '__main__':
    main()
