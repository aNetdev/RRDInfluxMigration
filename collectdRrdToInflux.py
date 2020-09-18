import sys,getopt
import re
import os
import rrdtool
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
	try:
		opts, args = getopt.getopt(argv,"hf:H:p:d:U:P:",["help=","folder=","host=","port=","database=","user=","password="])
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

	if dir == "" or db == "":
		print("ERROR: Missing or duplicated parameters.")
		help()
		sys.exit(2)
	
	client = InfluxDBClient(host, port, user, password, db)
	client.switch_database(db)

	for host in os.listdir(dir):
		for measure in os.listdir(os.path.join(dir,host)):
			measure_split = measure.split("-")
			for fileName in os.listdir(os.path.join(dir,host,measure)):
				fname =os.path.join(dir,host,measure,fileName)
				measure=measure_split[0]
				print(fname)
				if os.path.isfile(fname):
					t = re.sub('\.rrd$','',fileName)
					type_split =t.split("-")
					t=type_split[0]
					allvalues = rrdtool.fetch(
						fname,
						"AVERAGE",
						'-s', str(rrdtool.first(fname)),
						'-e', str(rrdtool.last(fname)))
					i=0
					start, end, step = allvalues[0]
					while i < len(allvalues[2]):
						val=allvalues[2][i][0]
						if (val != None):
							unixts=start + (step * 1+i)
							json_body = [
								{
									"measurement": measure + "_value",
									"time": unixts,
									"tags":{
										"host":host,
										"type":t,			
									},
									"fields": {															
										"value": val
									}
								}							
							]
							if(len(measure_split)>1):
								json_body[0]["tags"]["instance"]=measure_split[1]
							if(len(type_split)>1):
								json_body[0]["tags"]["type_instance"]=type_split[1]
							client.write_points(json_body, time_precision ="s", batch_size=10000)
						i=i+1


if __name__ == "__main__":
	main(sys.argv[1:])

