#!/usr/bin/env python
import argparse, yaml, consulate, json
from dictdiffer import diff

parser = argparse.ArgumentParser(description='Params for consul')
parser.add_argument('file', metavar='file', type=str, help='Yaml file path')
parser.add_argument('service_name', metavar='service_name', type=str, help='test_andrew')
parser.add_argument('host', metavar='host', type=str, nargs='?', help='Consul host name', default='localhost')
parser.add_argument('port', metavar='port', type=int, nargs='?', help='Consul port', default=80)
parser.add_argument('token', metavar='token', type=str, nargs='?', help='Consul token', default="-")
args = parser.parse_args()

c = consulate.Consul(host=args.host, port=args.port, token=args.token)
from_file = {}
existing = c.kv.find(args.service_name + "/", separator=None)


def process_data(rest):
    for k in list(rest):
        if k[0] == "change":
            print("Change Item, " + k[1] + ", change is: " + ' to '.join(k[2]))
            c.kv.set(k[1], k[2][1])
        elif k[0] == "remove":
            for kx in list(k[2]):
                print("Deleted Item :")
                print(kx[0])
                c.kv.delete(kx[0], recurse=False)
        elif k[0] == "add":
            for kx in list(k[2]):
                print "Add Item, {0}".format(': '.join(kx))
                c.kv.set(kx[0], kx[1])


def parse_data(d, key):
    for k, v in d.iteritems():
        if key != "":
            newkey = str(key) + "/" + str(k)
        else:
            newkey = str(key) + str(k)

        if isinstance(v, dict):
            parse_data(v, newkey)
        else:
            if isinstance(v, bool):
                val = str(v).lower()
            elif isinstance(v, int):
                val = str(v)
            elif isinstance(v, float):
                val = str(v)
            elif isinstance(v, list):
                val = json.dumps(v)
            else:
                val = str(v)

            from_file[args.service_name + "/" + newkey] = val


with open(args.file, 'r') as stream:
    try:
        data = yaml.load(stream)
        parse_data(data, "")
        result = diff(existing, from_file)
        process_data(result)

    except yaml.YAMLError as exc:
        print(exc)
