"""
VendorID			tpep_pickup_datetime	tpep_dropoff_datetime	passenger_count		trip_distance
pickup_longitude	pickup_latitude			RateCodeID				store_and_fwd_flag	dropoff_longitude	
dropoff_latitude	payment_type			fare_amount				extra				mta_tax				
tip_amount			tolls_amount			improvement_surcharge	total_amount
"""
from pyspark import SparkConf, SparkContext, SparkFiles
from operator import add
from shapely.geometry import MultiPoint, Point
import shapefile

APP_NAME = "First_Attempt"
#15/1/2015 19:23
#Only use hour and minute, converte minute to intervals of 10.
def parse(line):
	line = line.split(",")
	datetime = line[2].split()
	try:
		#hour = int(time[0])
		#minute = int(time[1])
		#minute -= minute % 10
		time = datetime[1]
		time = time[0:-4] + '0'
		x = float(line[9])
		y = float(line[10])
  	except:
  		print "*********************"
  		print "Invalid Point, line is:", line
  		#print "time is:", time
  		return ("Invalid Point", 1)
	#print "**************************"
	#print "newLine is:", newLine
	point = Point(x, y)
	county = "Not found"
	srs =shapeRecs.value
	for sr in srs:
		coords = sr.shape.points
		polygon = MultiPoint(coords).convex_hull
		if point.within(polygon):
			county = sr.record[6]
	newLine = (time + "," + county, 1)
	if county == "Not found":
		print "********************"
		print "County not found, point is:", x, y
	return newLine

def main(sc):
	trips = sc.textFile("test.csv").map(parse).reduceByKey(add).sortBy(lambda x:x[1], False)
	trips.saveAsTextFile("test_output")

if __name__ == "__main__":
	conf = SparkConf().setAppName(APP_NAME)
	sc = SparkContext(conf=conf)
	sc.addPyFile("shapefile.py")
	sc.addFile("../NY_counties_clip/NY_counties_clip.dbf")
	sc.addFile("../NY_counties_clip/NY_counties_clip.shx")
	sc.addFile("../NY_counties_clip/NY_counties_clip.shp")
	shapefilePath = SparkFiles.get("NY_counties_clip.shp")
	sf = shapefile.Reader(shapefilePath)
	shapeRecords = sf.shapeRecords()
	shapeRecs = sc.broadcast(shapeRecords)
	main(sc)