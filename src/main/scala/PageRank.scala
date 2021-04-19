import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.SparkConf

object PageRank {
  def main(args: Array[String]) {

    val spark_conf = new SparkConf()
      .setAppName("PageRank").setMaster("local[1]")

    // create sparkSession with spark-conf
    val spark = SparkSession
      .builder()
      .config(spark_conf)
      .getOrCreate()

    println(spark.conf.getAll)

    if (args.length != 3) {
      println("Usage: PageRank InputFile numIterations OutputDir")
      spark.stop()
      return
    }

    val filePath = args(0)
    val numIterations = args(1).toInt
    val carrierData = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(filePath)
    //    carrierData.take(2).foreach(println(_))
    val dataRDD = carrierData.select("ORIGIN", "DEST").rdd
    //    println(s"Data RDD : $dataRDD")
    val obj = new PageRank
    // pageranked nodes
    val prNodes = obj.run(dataRDD, numIterations)

    println("\nAirport codes and pagerank values")
    //print airport codes and pagerank values in descending order
    prNodes.map(n => (n.pr, n.id)).sortBy(t => (-t._1, t._2)).saveAsTextFile(args(2))
    //            .collect
    //            .foreach{case(pr, nid) => println("origin code: "+nid +" PageRank: "+pr)}
    //            .saveAsTextFile(args(2))

    spark.stop()
  }
}
class PageRank extends scala.Serializable{
  def defaultPr: Double = 10
  class Node extends scala.Serializable{
    // id of the node
    var id: String = _
    // no. of outlinks of the node
    var numOutLinks : Int =_
    // page rank value
    var pr: Double = _
    // list of neighbouring nodes
    var adj: List[String] = _

    // define auxiliary constructor (id, numOutLinks, adj)
    def this(i: String, c: Int, l: List[String]) {
      this()
      id = i
      numOutLinks = c
      adj = l
      pr = defaultPr
    }
    // can be used if id is set to private
    def setPr(i : Double){
      pr = i
    }
  }

  // dataRDD consists of row with origin and destination codes
  def run(dataRDD: RDD[Row], numIterations: Int): RDD[Node] ={

    // pairRDD of id and numOutLinks
    val outLinksCount = dataRDD.map(x => (x.get(0).toString, 1)).reduceByKey(_ + _).sortBy(-_._2)
    // pairRDD of id and adjacencyList
    val adjList = dataRDD.map(x =>(x.get(0).toString, List(x.get(1).toString))).reduceByKey(_ ::: _).sortBy(-_._2.size)

    // pairRDD (id, (numOutLinks, adj)) nodes with outlinks
    val nodeData = outLinksCount.join(adjList)

    // distinct origins and destinations
    val allOrig = adjList.map(x => x._1).distinct()
    val allDest = adjList.flatMap(x => x._2).distinct()

    // distinct nodes in overall graph
    val distNodes = allOrig.union(allDest).distinct

    // no. of Distinct nodes in graph
    val numDistNodes = distNodes.count
    println("Total Distinct nodes in graph= " + numDistNodes)
    //     println("printing nodeData")
    //     nodeData.collect().foreach(x => println(x))
    //     adjList.map(t => (t._1, (t._2.size, t._2))).collect().foreach(x => println(x))

    // dangling nodes i.e nodes without outlinks
    // Returns an RDD with the elements from (distNodes) that are not in (allOrig).
    val dangNodes = distNodes.subtract(allOrig).distinct
    //     dangNodes.map(n => (n,(0,List.empty[String])) ).collect().foreach(x => println(x))

    // all nodes in graph:    nodes  +  dangNodes
    var nodes = generateNodes(nodeData.union(dangNodes.map(n => (n,(0,List.empty[String])))))
    //     println("printing nodes")
    //     nodes.sortBy(_.id).foreach(x => println(x.id+" "+x.adj))

    println("/n Running prMapAndReduce for "+numIterations+" iterations")
    for (i <- (1 to numIterations)) {
      //       println("---------------Iteration "+i)
      var updatedNodes = prMapAndReduce(nodes, numDistNodes)
      // update the nodes for next  iteration
      nodes = updatedNodes
    }
    nodes
  }

  def generateNodes(nodeData: RDD[(String, (Int, List[String]))]): RDD[Node] ={

    val nodes = nodeData.map{case (ori,(cnt,list)) => new Node(ori, cnt, list)}
    println("No. of nodes generated= "+nodes.count)
    nodes
  }

  // print rdd
  def p(rdd: org.apache.spark.rdd.RDD[_]) = rdd.collect.foreach(println)

  /*
    https://lintool.github.io/MapReduceAlgorithms/MapReduce-book-final.pdf
    Page: 98 CHAPTER 5. GRAPH ALGORITHMS
  */

  // map and reduce to update PR
  def prMapAndReduce (nodes: RDD[Node], numDistNodes: Long): RDD[Node] ={
    // set jumping factor
    val alpha:Double = 0.15

    // total sum of dangling nodes before map and reduce
    val dangPr = nodes.filter(_.numOutLinks == 0).map(_.pr).sum
    //     println("total dang pr ="+dangPr)

    // sending pr mass to each adj node (Didn't include the node itself i.e Passing the graph structure)
    val flattened = nodes.flatMap{n => n.adj.map{y => (y, n.pr/n.numOutLinks.toDouble)} }

    // reduce to get updated pr value
    val reduced = flattened.reduceByKey(_+_)
    //     println("after reduce op on flattened= "+reduced)

    // join nodes and updated page ranks
    val ret = nodes.map(n => (n.id, n)).leftOuterJoin(reduced)

    val ret2 = ret.map{case(id,(n, updatedPr)) =>
      if (updatedPr.isEmpty){
        n.pr = 0
        n
      }
      else {
        n.pr = updatedPr.getOrElse(0)
        n
      }
    }

    // pr that was not passed on to any where : dangPr
    val addt2= dangPr/numDistNodes.toDouble
    // to ensure that the sum of probability to 1
    val addt1= (defaultPr * numDistNodes)*alpha/numDistNodes

    // update pr by distributing missing pagerank and adding jumping factor
    val ret3 = ret2.map(n => { var m = n.pr + addt2
      m *= (1- alpha)
      m += addt1
      n.pr = m
      n
    })

    //     println("\nEnd of iteration, printing top 5 PageRank Nodes")
    //     // sort by -pr , id, -numoutlinks
    //     ret3.map(n => (n, n.pr, n.id, n.numOutLinks)).sortBy(t => (-t._2,t._3, -t._4)).take(5).foreach{case(n,pr, nid,c) => println("ori "+n.id +" count "+n.numOutLinks+ " pr "+n.pr+ " listsize "+n.adj.size)}//+" node "+ n)
    ret3
  }
}
