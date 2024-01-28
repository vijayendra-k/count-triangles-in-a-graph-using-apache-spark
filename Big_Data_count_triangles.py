#By: Akshay Engolikar, Vijayendra Kosigi, Chinmaya Gokul Madhusudhan

# Install necessary packages
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q https://dlcdn.apache.org/spark/spark-3.3.3/spark-3.3.3-bin-hadoop3.tgz
!tar xf spark-3.3.3-bin-hadoop3.tgz
!pip install -q findspark
!pip install pyspark

# Set environment variables for Spark
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.3.3-bin-hadoop3"

# Initialize Spark
import findspark
findspark.init()
from pyspark import SparkContext
from google.colab import files

from operator import sub
# Upload files
files.upload()

# Create Spark context
sc = SparkContext.getOrCreate()

# Step 1: Load edges from the text file
edges_rdd = sc.textFile("triangle.txt")
print(edges_rdd.collect())

# Step 1: Convert each line to a tuple (u, v)
def line_to_tuple(line):
    values = line.split(', ')
    return tuple(map(int, values))

edgestotuples = edges_rdd.map(line_to_tuple)
print(edgestotuples.collect())

# Step 1: Map each edge to two key-value pairs, (u, v) and (v, u)
def edge_to_tuples(edge):
    return [(edge[0], edge[1]), (edge[1], edge[0])]

mappededges = edgestotuples.flatMap(edge_to_tuples)
print(mappededges.collect())

# Step 1: Group by key to get the list of neighbors for each node
neighborsgrouped = mappededges.groupByKey().mapValues(list)
neighborsgrouped.collect()

# Step 2: Generate triangle candidates by emitting (w1, w2) for each pair of neighbors of u
def generate_triangle_candidates(data):
    node, neighbors = data
    candidates = [((w1, w2), "to check") for w1 in neighbors for w2 in neighbors if w1 != w2]
    return candidates

triangle_candidates = neighborsgrouped.flatMap(generate_triangle_candidates)
print(triangle_candidates.collect())

# Step 2: Tag each present edge in the graph with "present edge"
present_edges = edgestotuples.map(lambda edge: (edge, "present edge"))
present_edges.collect()

# Step 2: Combine triangle candidates and present edges
all_edges = triangle_candidates.union(present_edges)
all_edges.collect()

# Step 2: Count triangles by checking if an edge is present and how many times it needs to be checked
def reduce_triangles(key, values):
    if "present edge" in values:
        return values.count("to check")

count_triangles = all_edges.groupByKey().mapValues(list).flatMap(lambda x: [reduce_triangles(x[0], x[1])])
count_triangles.collect()

# Step 2: Sum up the counts, each triangle is counted 3 times
total_triangles = count_triangles.filter(lambda x: x is not None).sum() // 3
total_triangles

# Step 3: Calculate the average number of triangles per node
total_nodes = neighborsgrouped.count()
avg_triangles_pn = total_triangles / total_nodes if total_nodes else 0

# Step 3: Print the total number of triangles and the average
print(f"The number of triangles on the graph is: {total_triangles}")
print(f"The average number of triangles per node is: {avg_triangles_pn}")