"""
VendorID			tpep_pickup_datetime	tpep_dropoff_datetime	passenger_count		trip_distance
pickup_longitude	pickup_latitude			RateCodeID				store_and_fwd_flag	dropoff_longitude	
dropoff_latitude	payment_type			fare_amount				extra				mta_tax				
tip_amount			tolls_amount			improvement_surcharge	total_amount
"""
from pyspark import SparkConf, SparkContext, SparkFiles
from operator import add
#from shapely.geometry import MultiPoint, Point, Polygon
import shapefile

APP_NAME = "Traffic_Peak_Analysis"

# determine if a point is inside a given polygon or not
# Polygon is a list of (x,y) pairs.
def point_inside_polygon(x,y,poly,bbox):
	maxX = max(bbox[0], bbox[2])
	minX = min(bbox[0], bbox[2])
	maxY = max(bbox[1], bbox[3])
	minY = min(bbox[1], bbox[3])
	if x < minX or x > maxX or y < minY or y > maxY:
		return False

	n = len(poly)
	inside = False
	p1x,p1y = poly[0]
	for i in range(n+1):
		p2x,p2y = poly[i % n]
		if y > min(p1y,p2y):
			if y <= max(p1y,p2y):
				if x <= max(p1x,p2x):
					if p1y != p2y:
						xinters = (y-p1y)*(p2x-p1x)/(p2y-p1y)+p1x
					if p1x == p2x or x <= xinters:
						inside = not inside
		p1x,p1y = p2x,p2y

	return inside

#15/1/2015 19:23
#Only use hour and minute, converte minute to intervals of 10.
def parse(line):
	#line = line.split(",")
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
	#point = Point(x, y)
	county = "Not found"
	srs =shapeRecs.value
	for sr in srs:
		coords = sr.shape.points
		bbox = sr.shape.bbox
		"""
		#polygon = MultiPoint(coords).convex_hull
		polygon = Polygon(coords)
		if point.within(polygon):
			county = sr.record[6]
		"""
		if point_inside_polygon(x,y,coords, bbox):
			county = sr.record[6]
		
	#if county == "Not found":
		#county = "Richmond"
		#print "********************"
		#print "County not found, point is:", x, y
	newLine = (time + "," + county, 1)
	return newLine

def deleteInvalidLines(line):
	try:
		int(line[0])
	except:
		return False
	return line[9] != "0.0" and line[10] != "0.0"

def main(sc):
	trips = sc.textFile("/taxidata/*/").map(lambda line:line.split(",")).filter(deleteInvalidLines)
	sortedTrips = trips.map(parse).reduceByKey(add).sortBy(lambda x:x[1], False)
	sortedTrips.saveAsTextFile("test_output")

if __name__ == "__main__":
	conf = SparkConf()
	conf.setAppName(APP_NAME)
	conf.setMaster('yarn-client')
	#conf.setMaster('local[*]')
	sc = SparkContext(conf=conf)
	sc.addPyFile("shapefile.py")
	#sc.addFile("../NY_counties_clip/NY_counties_clip.dbf")
	#sc.addFile("../NY_counties_clip/NY_counties_clip.shx")
	#sc.addFile("../NY_counties_clip/NY_counties_clip.shp")
	#shapefilePath = SparkFiles.get("NY_counties_clip.shp")
	shapefilePath = "../NY_counties_clip/NY_counties_clip"
	sf = shapefile.Reader(shapefilePath)
	shapeRecords = sf.shapeRecords()
	shapeRecs = sc.broadcast(shapeRecords)
	main(sc)
