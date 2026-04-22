
from pyspark import SparkContext

sc = SparkContext(appName="Assignment2")

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

movies = sc.textFile("movies.txt")     .map(lambda x: x.split(","))     .map(lambda x: (x[0], x[2].split("|")))

ratings = sc.textFile("ratings_1.txt")     .union(sc.textFile("ratings_2.txt"))     .map(lambda x: x.split(","))     .map(lambda x: (x[1], float(x[2])))

joined = ratings.join(movies)

genre_ratings = joined.flatMap(
    lambda x: [(g, (x[1][0],1)) for g in x[1][1]]
)

agg = genre_ratings.reduceByKey(lambda a,b:(a[0]+b[0], a[1]+b[1]))
result = agg.mapValues(lambda x: round(x[0]/x[1],2)).sortBy(lambda x: x[0])

top = result.take(20)
print_table(top, ["Genre","AvgRating"])

output = result.map(lambda x: f"{x[0]},{x[1]}")
output.coalesce(1).saveAsTextFile("assignment2/output")
