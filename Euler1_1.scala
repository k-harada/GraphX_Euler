# in spark-shell, Spark context available as sc.

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

# spark-shellではなくてもいいらしい
# val conf = new SparkConf().setMaster("local").setAppName("Euler1")
# val sc = new SparkContext(conf)

val nodes: RDD[(VertexId,Int)] = sc.parallelize(Array((0L,1),(1L,0)))
val edges: RDD[Edge[Int]] = sc.parallelize(Array(Edge(0L,0L,0),Edge(0L,1L,1)))
val graph1 = Graph(nodes,edges).cache()


val graph2 = Pregel(graph1,0,999,EdgeDirection.Out)(
(id,attr,msg) => attr+msg,
edge => Iterator((edge.dstId,if (edge.attr==0) {
1
} else if (edge.srcAttr % 3 ==0 || edge.srcAttr % 5 ==0 ){
edge.srcAttr
} else 0
)),
(a,b) => a+b
)

graph2.vertices.collect.foreach(println(_))
