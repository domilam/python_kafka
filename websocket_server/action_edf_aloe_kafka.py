#!/usr/bin/env python3.6
import site
site.addsitedir('/opt/projects/common/path')
from wsaction import WsAction
from stats_supervision import StatsSupervision
from streamdatas_kafka import VtscadaRealTime, values_to_front
from esmessage import EsMessage

#________________________________________________________ WsActionEdfAloe
class WsActionEdfAloe(WsAction):

    flag_stop = None
    dic_batiment = {'ADMI': 'batiment Administratif', 'COMM': 'batiment Commande', 'SHOW': 'Showroom'}
    dic_etage = {'RDC': 'rez-de-chaussée', 'ET1': 'étage 1', 'ET2': 'étage 2', }
    def graph_stat(self):
        err = self.check_options(['batiment', 'etage', 'date_debut', 'date_fin', 'froid', 'total'])
        if err is not None:
            yield None, err
            return
        stat_sup = StatsSupervision()
        stat_sup()
        batiment = self._options['batiment']
        batiment = batiment if batiment != '' else None

        etage = self._options['etage']
        etage = etage if etage != '' else None

        date_debut = self._options['date_debut']
        date_debut = date_debut if date_debut != '' else None

        date_fin = self._options['date_fin']
        date_fin = date_fin if date_fin != '' else None

        froid = self._options['froid']
        froid = froid if froid != '' else None
        
        total = self._options['total']
        total = total if total != '' else None

        obj_graph = stat_sup.stat_to_front(batiment, etage, date_debut, date_fin, froid, total)
        yield obj_graph, 'ok'

    def test(self):
        value = self['value']
        yield {"value": [value, 2, 34]}, 'ok'

    def graph_stream(self):
        err = self.check_options(['tag', 'reel'])
        if err is not None:
            yield None, err
            return
        reel = self._options['reel']
        tag_recu = self._options['tag']
        WsActionEdfAloe.flag_stop = False
        realtime = VtscadaRealTime(coll_mesure='mesures_stream')
        realtime.run_streaming()
        n = 0
        ind_element = 0
        send_graph = False
        list_xy = []

        if isinstance(tag_recu, str):
            t_split = tag_recu.split('/')
            if len(t_split) >= 2:
                if 'PV' in t_split:
                    type_data = 'production'
                else:
                    type_data = 'consommation'
                id = t_split[0] + '_' + t_split[1]
                name_addition = WsActionEdfAloe.dic_batiment[t_split[0]] + ' ' + WsActionEdfAloe.dic_etage[t_split[1]]
                graph = {
                    'id': id,
                    'name': 'Graphe Temps réel de la {} du {}'.format(type_data, name_addition),
                    'mode': 'time',
                    'data': []
                }
                for element in values_to_front(reel):
                    if WsActionEdfAloe.flag_stop:
                        return
                    nb_tag = 1
                    retour = None
                    for tag in element:
                        # if tag == 'ADMI/RDC' and element[tag] is not None:
                        if element[tag] is not None:
                            if element[tag][1] is not None and element[tag][2] == tag_recu:
                                if not send_graph and n != nb_tag:
                                    graph['data'].append({
                                        'type': "lines",
                                        'ylabel': 'type_data',
                                        'Y': {
                                            "dataY": [element[tag][1]],
                                            "unit": "kWh",
                                        },
                                        'X': {
                                            "dataX": [element[tag][0]],
                                            "unit": 'Date/heure'
                                        },
                                        'xlabel': 'date',
                                        'legend': "",
                                        'color': "#FF0022",
                                        'points': True
                                    })
                                    n += 1
                                    ind_element += 1
                                    print(tag)

                                else:
                                    if not send_graph:
                                        print(ind_element)
                                        graph['data'][0]['X']['dataX'].append(element[tag][0])
                                        graph['data'][0]['Y']['dataY'].append(element[tag][1])
                                        retour = {'obj_graph': graph}
                                        n += 1
                                        ind_element += 1
                                        print(tag)
                                    else:
                                        list_xy.append([element[tag][0]*1000, element[tag][1]])
                                        retour = {'id': id, 'data': list_xy}
                                        ind_element += 1
                                        print(tag)
                        if ind_element == 2:
                            if not send_graph:
                                send_graph = True
                            ind_element = 0
                            list_xy = []
                            yield retour, 'ok'
                    # if not send_graph and n != nb_tag:
                    #     n = 0
                    #     graph['data'] = []
                            # time.sleep
        else:
            yield None, 'ok'

    def stream_values(self):
        err = self.check_options(['tags', 'reel'])
        if err is not None:
            yield None, err
            return
        my_message = EsMessage()
        tags = self._options['tags']
        reel = self._options['reel']
        if type(tags) == list:
            WsActionEdfAloe.flag_stop = False
            realtime = VtscadaRealTime(coll_mesure='mesures_stream')
            realtime.run_streaming()
            for element in values_to_front(reel):
                if WsActionEdfAloe.flag_stop:
                    return
                retour = {}
                list_tags = []
                for tag in element:
                    if tag in tags:
                        if element[tag] is not None:
                            if element[tag][1] is not None:
                                list_tags.append({tag: element[tag][1]})
                                retour['obj_stream_values'] = list_tags
                                message = "ok"
                                type_message = 'success'
                                message_format = my_message.get_message(message, type_message)
                                retour = {'obj_stream_values': list_tags, 'message': message_format}
                                # print(retour)
                if bool(retour):
                    yield retour, 'ok'
            else:
                message = "tags doit être une list"
                type_message = 'warning'
                message_format = my_message.get_message(message, type_message)
                retour = {'obj_stream_values': None, 'message': message_format}
                return retour, 'ok'

    def stream_stop(self):
        WsActionEdfAloe.flag_stop = True
        yield 'stop', 'ok'

# ==============================================
if __name__ == '__main__':
    import json
    import sys

    if len(sys.argv) >= 2:
        arg = sys.argv[1]
        options = json.loads(arg)
        # options = {"action": "test", "value": 10}
        my_action = WsActionEdfAloe()
        for data, msg in my_action(options):
            msg = json.dumps(data)
            print(msg)
    else:
        choice = 'graph_stream'
        if choice == 'graph_stat':
            options = {"action": "graph_stat", "batiment": "SHOW", "etage": None, "date_debut": "2017-01-11", "date_fin": "2017-01-13", "froid": "only", "total": False}
        elif choice == 'graph_stream':
            options = {'action': 'graph_stream', 'tag': 'ADMI/RDC', 'reel': True}
        elif choice == 'graph_stream_fake':
            options = {'action': 'graph_stream', 'tag': 'ADMI/RDC', 'reel': False}
        elif choice == 'test':
            options = {'action': 'test', 'value': 10}
        elif choice == 'stream_values_fake':
            options = {'action': 'stream_values', 'tags': ['ADMI/RDC', 'ADMI/ET1'], 'reel': False}
        elif choice == 'stream_values':
            options = {'action': 'stream_values', 'tags': ['ADMI/RDC', 'ADMI/ET1'], 'reel': True}

        my_action = WsActionEdfAloe()
        test_action = WsActionEdfAloe()
        for data, msg in my_action(options):
            print(data, msg)
