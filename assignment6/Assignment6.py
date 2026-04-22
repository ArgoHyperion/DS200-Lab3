
from pyspark import SparkContext
from datetime import datetime

sc = SparkContext(appName="Assignment6")

def print_table(data, headers):
    data_str = [[str(x) for x in row] for row in data]
    cols = list(zip(*([headers] + data_str)))
    col_widths = [max(len(item) for item in col) for col in cols]
    def format_row(row):
        return " | ".join(val.ljust(width) for val, width in zip(row, col_widths))
    print(format_row(headers))
    print("-+-".join('-'*w for w in col_widths))
    for row in data_str:
        print(format_row(row))

def get_year(ts):
    return datetime.fromtimestamp(int(ts)).year

ratings = sc.textFile("ratings_1.txt")     .union(sc.textFile("ratings_2.txt"))     .map(lambda x: x.split(","))     .map(lambda x: (get_year(x[3]), (float(x[2]),1)))

agg = ratings.reduceByKey(lambda a,b:(a[0]+b[0], a[1]+b[1]))

result = agg.map(lambda x: (x[0], round(x[1][0]/x[1][1],2), x[1][1]))             .sortBy(lambda x: x[0])

top = result.take(20)
print_table(top, ["Year","AvgRating","Count"])

output = result.map(lambda x: f"{x[0]},{x[1]},{x[2]}")
output.coalesce(1).saveAsTextFile("assignment6/output")

