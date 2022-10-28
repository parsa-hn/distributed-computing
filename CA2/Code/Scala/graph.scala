val data = sc.textFile("/home/parsa/Desktop/Distributed Systems/Cloud Computing HW/mygraph.txt")
val splitData = data.map(line => line.split("\\s+"))

val outNodesData = splitData.map(edge => (edge(0), -1 * edge(2).toInt))

val inNodesData = splitData.map(edge => (edge(1), edge(2).toInt))

val nodesData = outNodesData.union(inNodesData)

val reduceData = nodesData.reduceByKey(_+_)

val reduceDataAbs = reduceData.map(reducedDatum => (reducedDatum._1, reducedDatum._2.abs))

val output = reduceDataAbs.filter(record => record._2 % 2 == 1)

output.saveAsTextFile("/home/parsa/Desktop/Distributed Systems/Cloud Computing HW/Scala/output")