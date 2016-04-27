from shapely.geometry import MultiPoint, Point

def parse(line):
	import shapefile
	line = line.split(",")
	datetime = line[2].split()
	try:
		#hour = int(time[0])
		#minute = int(time[1])
		#minute -= minute % 10
		time = datetime[1]
		time = time[0:-1] + '0'
		x = float(line[9])
		y = float(line[10])
  	except:
  		print "*********************"
  		print "Invalid Point, line is:", line
  		#print "time is:", time
  		return ("Invalid Point", 1)
	#print "**************************"
	#print "newLine is:", newLine
	sf = shapefile.Reader("../NY_counties_clip/NY_counties_clip")
	shapeRecs = sf.shapeRecords()
	point = Point(x, y)
	county = "Not found"
	for sr in shapeRecs:
		coords = sr.shape.points
		polygon = MultiPoint(coords).convex_hull
		if point.within(polygon):
			county = sr.record[6]
	newLine = (time + "," + county, 1)
	if county == "Not found":
		print "********************"
		print "County not found, point is:", x, y
	return newLine

if __name__ == "__main__":
	testFile = open("../test.csv", "r")
	for line in testFile:
		parse(line)

"""
point = Point(-73.993896484375,40.750110626220703)
county = "Not found"
zones = set()
for sr in shapeRecs:
	coords = sr.shape.points
	polygon = MultiPoint(coords).convex_hull
	if point.within(polygon):
		county = sr.record[6]
	zones.add(sr.record[6])
	if sr.record[6] == "Richmond County":
		print "the points of Richmond county:", coords
print county
print sorted(zones)
"""