from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[2]").setAppName("PropertyValues")
sc = SparkContext(conf = conf)

# pure python function to parse, clean and extract data
def parseLine(line):
    fields = line.split(',')
    currentValue = float(fields[7].replace('$',''))
    elementarySchool = fields[25]
    return (elementarySchool, currentValue)

# data file can be downloaded from this URL:
# https://data.cityofmadison.com/Property/Assessor-Property-Information/u7ns-6d4x

# load data file
lines = sc.textFile("file:///home/pitt/Documents/Assessor_Property_Information.csv")

# extract the key value pair fromthe file using a defined function
parsedLines = lines.map(parseLine)

# show an example of filtering data out using a lambda function
actualValues = parsedLines.filter(lambda x: "Mc Farland" not in x[0])

# add a count to the second data element, then reduceByKey
# to obtain a sum and count for each key.
totalsBySchool = actualValues.mapValues(lambda x: (x, 1)) \
  .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# calculate an average (sum/count) for each key
averagesBySchool = totalsBySchool.mapValues(lambda x: x[0] / x[1])

# collect the results back at the driver
results = averagesBySchool.collect()

# print the results out to the screen
for result in results:
  print result[0] + " | " + "\t{:.2f}".format(result[1])

# to run this from the command line:
# ./bin/spark-submit ~/Documents/presentation/PropertyValues.py
