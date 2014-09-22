// As I run in spark-shell, Spark context available as sc.

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// val conf = new SparkConf().setMaster("local").setAppName("Euler1")
// val sc = new SparkContext(conf)

val graph1 = GraphLoader.edgeListFile(sc,"GraphX_Euler/edge1.tsv").cache()
// for文で入れたいけどよくわかんないので安心のtsv入力
// 1 0 1
// 2 0 1
//
// 999 0 1

// 1から999が0に向かってedgeを出す

val graph2 = graph1.mapVertices((id,attr) => id.toInt) // Verticesのattrを自分のidで初期化する

val graph3 = Pregel(graph2,0,100,EdgeDirection.Out)(
  (id,attr,msg) => attr+msg,
  edge => Iterator((edge.dstId,if (edge.srcAttr % 3 ==0 || edge.srcAttr % 5 ==0 ){
  edge.srcAttr // Verticesのattrが3の倍数or5の倍数ならattr、そうでなければ0を流す
  } else 0 
  )),
  (a,b) => a+b
)

graph3.vertices.min // Id0のattrを呼び出したいけどよくわかんない


