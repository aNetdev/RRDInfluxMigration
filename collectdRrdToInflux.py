#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys,getopt
import re
import os
import rrdtool
import xml.etree.ElementTree as ET
import pprint
from influxdb import InfluxDBClient


def main(argv):

	RRD_MIN_RES=10

	update=False
	dump=False
	dir=""
	host="localhost"
	port="8086"
	db=""
	key=""
	user=""
	password=""
	device=""

	def help():
		print('Usage: collectdRrdToInflux.py [-u|-m] -f <RRD FILE> [-H <INFLUXDB HOST>] [-p <INFLUXDB PORT>] -d DATABASE [-U user] [-P password] [-k KEY] -D device [-h] ')
		print('Updates or dumps passed RRD File to selected InfluxDB database')
		print('	-h, --help		Display help and exit')
		print('	-f, --folder		folder containing collectd rrd files to dump')
		print('	-H, --host		Optional. Name or IP of InfluxDB server. Default localhost.')
		print('	-p, --port		Optional. InfluxDB server port. Default 8086.')
		print('	-d, --database		Database name where to store data.')
		print('	-U, --user		Optional. Database user.')
		print('	-P, --password		Optional. Database password.')
		print('	-D, --device		Device the RRD metrics are related with.')
	try:
		opts, args = getopt.getopt(argv,"humf:H:p:d:U:P:k:D:",["help=","update=","dump=","file=","host=","port=","database=","user=","password=","key=","device="])
	except getopt.GetoptError as err:
		help()
		sys.exit(2)

	for opt, arg in opts:
		if opt == '-h':
			help()
			sys.exit()      
		elif opt in ("-f", "--folder"):
			dir = arg
		elif opt in ("-H", "--host"):
			host = arg
		elif opt in ("-p", "--port"):
			port = arg
		elif opt in ("-d", "--database"):
			db = arg
		elif opt in ("-U", "--user"):
			user = arg
		elif opt in ("-P", "--password"):
			password = arg		
		elif opt in ("-D", "--device"):
			device = arg

	if device == "" or dir == "" or db == "":
		print("ERROR: Missing or duplicated parameters.")
		help()
		sys.exit(2)
	
	client = InfluxDBClient(host, port, user, password, db)
	client.switch_database(db)

	for host in os.listdir(dir):
		for measure in os.listdir(os.path.join(dir,host)):
			for fileName in os.listdir(os.path.join(dir,host,measure)):
				fname =os.path.join(dir,host,measure,fileName)
				print(fname)
				if os.path.isfile(fname):
					t = re.sub('\.rrd$','',fileName)
					split =t.split("-")
					t=split[0]

					allvalues = rrdtool.fetch(
						fname,
						"AVERAGE",
						'-e', str(rrdtool.last(fname)-RRD_MIN_RES),
						'-r', str(RRD_MIN_RES))
					i=0
					while i < len(allvalues[2]):
						val=allvalues[2][i][0]
						unixts=allvalues[0][0]+(i+1)*RRD_MIN_RES
						json_body = [
							{
								"measurement": measure + "_value",
								"time": unixts,
								"fields": {
									"host":device,
									"type":t,									
									"value": val
								}
							}							
						]
						if(len(split)>1):
							json_body[0]["fields"]["type_instance"]=split[1]
						client.write_points(json_body)
						i=i+1


if __name__ == "__main__":
	main(sys.argv[1:])

