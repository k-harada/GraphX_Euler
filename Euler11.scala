//上の 20×20 の格子のうち, 斜めに並んだ4つの数字が赤くマークされている.
//
//08 02 22 97 38 15 00 40 00 75 04 05 07 78 52 12 50 77 91 08
//49 49 99 40 17 81 18 57 60 87 17 40 98 43 69 48 04 56 62 00
//81 49 31 73 55 79 14 29 93 71 40 67 53 88 30 03 49 13 36 65
//52 70 95 23 04 60 11 42 69 24 68 56 01 32 56 71 37 02 36 91
//22 31 16 71 51 67 63 89 41 92 36 54 22 40 40 28 66 33 13 80
//24 47 32 60 99 03 45 02 44 75 33 53 78 36 84 20 35 17 12 50
//32 98 81 28 64 23 67 10 26 38 40 67 59 54 70 66 18 38 64 70
//67 26 20 68 02 62 12 20 95 63 94 39 63 08 40 91 66 49 94 21
//24 55 58 05 66 73 99 26 97 17 78 78 96 83 14 88 34 89 63 72
//21 36 23 09 75 00 76 44 20 45 35 14 00 61 33 97 34 31 33 95
//78 17 53 28 22 75 31 67 15 94 03 80 04 62 16 14 09 53 56 92
//16 39 05 42 96 35 31 47 55 58 88 24 00 17 54 24 36 29 85 57
//86 56 00 48 35 71 89 07 05 44 44 37 44 60 21 58 51 54 17 58
//19 80 81 68 05 94 47 69 28 73 92 13 86 52 17 77 04 89 55 40
//04 52 08 83 97 35 99 16 07 97 57 32 16 26 26 79 33 27 98 66
//88 36 68 87 57 62 20 72 03 46 33 67 46 55 12 32 63 93 53 69
//04 42 16 73 38 25 39 11 24 94 72 18 08 46 29 32 40 62 76 36
//20 69 36 41 72 30 23 88 34 62 99 69 82 67 59 85 74 04 36 16
//20 73 35 29 78 31 90 01 74 31 49 71 48 86 81 16 23 57 05 54
//01 70 54 71 83 51 54 69 16 92 33 48 61 43 52 01 89 19 67 48
//それらの数字の積は 26 × 63 × 78 × 14 = 1788696 となる.
//
//上の 20×20 の格子のうち, 上下左右斜めのいずれかの方向で連続する4つの数字の積のうち最大のものはいくつか?

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


//  0   1   2  .  .  .  21
// 22  23  24  .  .  .  43
// 44  45  46  .  .  .  65
//  .   .   .  .  .  .   .
//  .   .   .  .  .  .   .
//  .   .   .  .  .  .   .
//462 463 464  .  .  . 483

// 外周は0を埋めて、真ん中の20×20マスが問題の数値を持つマス
// Pregelで流してリストで連続する4つの数を残して、最後に積を取ってmax

// Vertex

val s = """08 02 22 97 38 15 00 40 00 75 04 05 07 78 52 12 50 77 91 08
49 49 99 40 17 81 18 57 60 87 17 40 98 43 69 48 04 56 62 00
81 49 31 73 55 79 14 29 93 71 40 67 53 88 30 03 49 13 36 65
52 70 95 23 04 60 11 42 69 24 68 56 01 32 56 71 37 02 36 91
22 31 16 71 51 67 63 89 41 92 36 54 22 40 40 28 66 33 13 80
24 47 32 60 99 03 45 02 44 75 33 53 78 36 84 20 35 17 12 50
32 98 81 28 64 23 67 10 26 38 40 67 59 54 70 66 18 38 64 70
67 26 20 68 02 62 12 20 95 63 94 39 63 08 40 91 66 49 94 21
24 55 58 05 66 73 99 26 97 17 78 78 96 83 14 88 34 89 63 72
21 36 23 09 75 00 76 44 20 45 35 14 00 61 33 97 34 31 33 95
78 17 53 28 22 75 31 67 15 94 03 80 04 62 16 14 09 53 56 92
16 39 05 42 96 35 31 47 55 58 88 24 00 17 54 24 36 29 85 57
86 56 00 48 35 71 89 07 05 44 44 37 44 60 21 58 51 54 17 58
19 80 81 68 05 94 47 69 28 73 92 13 86 52 17 77 04 89 55 40
04 52 08 83 97 35 99 16 07 97 57 32 16 26 26 79 33 27 98 66
88 36 68 87 57 62 20 72 03 46 33 67 46 55 12 32 63 93 53 69
04 42 16 73 38 25 39 11 24 94 72 18 08 46 29 32 40 62 76 36
20 69 36 41 72 30 23 88 34 62 99 69 82 67 59 85 74 04 36 16
20 73 35 29 78 31 90 01 74 31 49 71 48 86 81 16 23 57 05 54
01 70 54 71 83 51 54 69 16 92 33 48 61 43 52 01 89 19 67 48"""

val ns = s.split("\\s+").map(_.toInt)

val verattr = for (x <- 0 to 21; y <- 0 to 21; c = if(x==0||x==21||y==0||y==21) 0 else ns(x-1+20*(y-1))) yield ((x+y*22)toLong,List(c))
// LongにしないとVertexIdと認識してくれない

val vertex: RDD[(VertexId,List[Int])] = sc.parallelize(verattr.to[collection.Seq])
// immutableとかで型がなかなか合わないので注意

// Edge
// for + yieldで頑張る
// right direction
val ed_r = for (x <- 0 to 20; y <- 0 to 21) yield Edge(x+y*22,x+y*22+1,"right")
val edge_r: RDD[Edge[String]] = sc.parallelize(ed_r)
// down direction
val ed_d = for (x <- 0 to 21; y <- 0 to 20) yield Edge(x+y*22,x+y*22+22,"down")
val edge_d: RDD[Edge[String]] = sc.parallelize(ed_d)
// downright direction
val ed_dr = for (x <- 0 to 20; y <- 0 to 20) yield Edge(x+y*22,x+y*22+23,"downright")
val edge_dr: RDD[Edge[String]] = sc.parallelize(ed_dr)
// downleft direction
val ed_dl = for (x <- 1 to 21; y <- 0 to 20) yield Edge(x+y*22,x+y*22+21,"downleft")
val edge_dl: RDD[Edge[String]] = sc.parallelize(ed_dl)

// Graph
val graph_r = Graph(vertex, edge_r).cache()
val graph_d = Graph(vertex, edge_d).cache()
val graph_dr = Graph(vertex, edge_dr).cache()
val graph_dl = Graph(vertex, edge_dl).cache()


// まず自分の数字を流して、以降は直近流れてきたものを流す
// 最初は特別に処理しないと初期メッセージ1が流れちゃうのでif then処理
val maxr = Pregel(graph_r,1,3,EdgeDirection.Out)(
(id,attr,msg) => if (attr.length == 1) msg+:attr else attr:+msg,
edge => Iterator((edge.dstId,edge.srcAttr.last)),
(a,b) => a*b
).vertices.collect.map(x => x._2.product).max

val maxd = Pregel(graph_d,1,3,EdgeDirection.Out)(
(id,attr,msg) => if (attr.length == 1) msg+:attr else attr:+msg,
edge => Iterator((edge.dstId,edge.srcAttr.last)),
(a,b) => a*b
).vertices.collect.map(x => x._2.product).max

val maxdr = Pregel(graph_dr,1,3,EdgeDirection.Out)(
(id,attr,msg) => if (attr.length == 1) msg+:attr else attr:+msg,
edge => Iterator((edge.dstId,edge.srcAttr.last)),
(a,b) => a*b
).vertices.collect.map(x => x._2.product).max

val maxdl = Pregel(graph_dl,1,3,EdgeDirection.Out)(
(id,attr,msg) => if (attr.length == 1) msg+:attr else attr:+msg,
edge => Iterator((edge.dstId,edge.srcAttr.last)),
(a,b) => a*b
).vertices.collect.map(x => x._2.product).max

List(maxr, maxd, maxdr, maxdl).max
