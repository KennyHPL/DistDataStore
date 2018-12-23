import os
import sys
import requests
import time
import unittest
import json

IPs = ['20', '21', '22', '23']
buildTag = "ptest"
ip_pref = '10.0.0.'
sudo = 'sudo'
port_pref = '808'
subnet = 'mynet'
num_shards = '2'
sleep_time = 3

def storeKeyValue(ipPort, key, value, payload):
    #print('PUT: http://%s/keyValue-store/%s'%(str(ipPort), key))
    return requests.put( 'http://%s/keyValue-store/%s'%(str(ipPort), key), data={'val':value, 'payload': payload})

def getKeyValue(ipPort, key, payload):
    #print('GET: http://%s/keyValue-store/%s'%(str(ipPort), key))
    return requests.get( 'http://%s/keyValue-store/%s'%(str(ipPort), key), data={'payload': payload} )

def getAllShardIds(ipPort):
    return requests.get( 'http://%s/shard/all_ids'%str(ipPort) )

def getMembers(ipPort, ID):
    return requests.get( 'http://%s/shard/members/%s'%(str(ipPort), str(ID)) )

def changeShardNumber(ipPort, newNumber):
    return requests.put( 'http://%s/shard/changeShardNumber'%str(ipPort), data={'num' : newNumber} )

class Tests(unittest.TestCase):


    def setUp(self):
        for idx, ip in enumerate(IPs):
            dock_run = f'docker run -d --ip={ip_pref}{ip} -p {port_pref}{idx+1}:{port_pref}0 --net={subnet}'
            view = ''
            for _ip in IPs:
                view += ip_pref+_ip+':'+port_pref+'0'+','
            view = view[:-1]
            env_view = f'-e VIEW=\"{view}\"'
            env_ip = f'-e IP_PORT=\"{ip_pref}{ip}:{port_pref}0\"'
            env_shard = f'-e S=\"{num_shards}\"'
            cmd = " ".join([sudo, dock_run, env_view, env_ip, env_shard, buildTag])
            print(cmd)
            os.system(cmd)

    def tearDown(self):
        cmd = " ".join([sudo, './build.sh rm'])
        os.system(cmd)

    payload = ''
    def test_1_insert_then_get(self):
        #inserting a key
        rsp = storeKeyValue('10.0.0.23:8080', 'hello', 'world', '')
        rsp_json = rsp.json()
        self.assertEqual(int(rsp.status_code), 200)
        self.assertEqual(rsp_json["replaced"], "False")
        self.assertEqual(rsp_json["msg"], "Added successfully")
        self.payload = rsp_json["payload"]
        print(rsp_json)

        #getting the inserted key
        rsp = getKeyValue('10.0.0.20:8080', 'hello', self.payload)
        rsp_json = rsp.json()
        self.assertEqual(int(rsp.status_code), 200)
        self.assertEqual(rsp_json["value"], "world")
        self.assertEqual(rsp_json["result"], "Success")
        print(rsp_json)

    def test_2_get_shards(self):
        rsp = getAllShardIds('10.0.0.20:8080')
        rsp_json = rsp.json()
        shard_ids = rsp_json["shard_ids"].split(',')
        members = []
        for id in shard_ids:
            rsp = getMembers('10.0.0.20:8080',id)
            print(rsp.json())
            self.assertTrue(len(rsp.json()['members'].split(',')) > 1)

    def test_3_reshard(self):
        rsp = changeShardNumber('10.0.0.21:8080', 1)
        print(rsp.json())

if __name__ == '__main__':
    unittest.main()
