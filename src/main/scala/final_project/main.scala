package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import scala.math.pow

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def compute(g_in: Graph[Int, Int]): Graph[Int,Int] = {
    var counter=0
    val random_num=scala.util.Random
    var active_edges=g_in.edges.count
    val initial=g_in.edges.count
    val temp=g_in.mapEdges(attr=>(random_num.nextFloat,false,false))
    var graph=temp.mapVertices[(Float,Boolean,Int,Int,Boolean)]((id,attr)=>(-1.toFloat,true,-1,-1,true)) //random float to store the max edge,boolean true or false, selected dst vertex id,selected src vertex id, is it still active
    while (counter<5){
      //active_edges>(initial.toFloat*0.005).toInt
      counter = counter+1
      //generate new set of random floats for each edges
      graph=graph.mapEdges(edge=>(random_num.nextFloat,edge.attr._2,edge.attr._3))

      //find the edge with the biggest random float for each source vertex then send the dst id to source
      var local=graph.aggregateMessages[(Float,Boolean,Int,Int,Boolean)](msg=>{
        if (msg.srcAttr._5==false || msg.dstAttr._5==false){
          msg.sendToSrc(
            (-1.toFloat,msg.srcAttr._2,-1,-1,msg.srcAttr._5)
          )
          msg.sendToDst(
            (-1.toFloat,msg.dstAttr._2,-1,-1,msg.dstAttr._5)
          )
        } else {
          msg.sendToSrc(
            (msg.attr._1,msg.srcAttr._2,msg.dstId.toInt,msg.srcAttr._4,msg.srcAttr._5)
          )
          msg.sendToDst(
            (msg.attr._1,msg.dstAttr._2,msg.srcId.toInt,msg.dstAttr._4,msg.dstAttr._5)
          )
        }},(a,b)=>{
        if (a._1>b._1){
          (a._1,a._2,a._3,a._4,a._5)
        } else{
          (b._1,b._2,b._3,b._4,b._5)
        }
      }
      )
      graph=Graph(local,graph.edges)//,edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      local=graph.aggregateMessages[(Float,Boolean,Int,Int,Boolean)](msg=>{
        if (msg.srcAttr._5==false || msg.dstAttr._5==false){
          msg.sendToDst(
            (-1.toFloat,msg.dstAttr._2,-1,-1,msg.dstAttr._5)
          )
          msg.sendToSrc(
            (-1.toFloat,msg.srcAttr._2,-1,-1,msg.srcAttr._5)
          )
        } else if (msg.dstId==msg.srcAttr._3 || msg.srcId==msg.dstAttr._3) {
          msg.sendToDst(
            (msg.attr._1,msg.dstAttr._2,msg.dstAttr._3,msg.srcId.toInt,msg.dstAttr._5)
          )
          msg.sendToSrc(
            (msg.attr._1,msg.dstAttr._2,msg.dstAttr._3,msg.dstId.toInt,msg.dstAttr._5)
          )
        } else{
          msg.sendToDst(
            (-1.toFloat,msg.dstAttr._2,-1,-1,msg.dstAttr._5)
          )
          msg.sendToSrc(
            (-1.toFloat,msg.srcAttr._2,-1,-1,msg.srcAttr._5)
          )
        }
      },(a,b)=>{
        if (a._1>b._1){
          (a._1,a._2,a._3,a._4,a._5)
        } else{
          (b._1,b._2,b._3,b._4,b._5)
        }
      }
      )

      graph=Graph(local,graph.edges)//,edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      //determine which edges are proposed
      graph=graph.mapTriplets(edge=>{
        if((((edge.srcAttr._3==edge.dstId) && (edge.dstAttr._4==edge.srcId)) || ((edge.dstAttr._3==edge.srcId) && (edge.srcAttr._4==edge.dstId)))&& (edge.attr._2==false)){
          (edge.attr._1,true,edge.attr._3)
        } else{
          (edge.attr._1,edge.attr._2,edge.attr._3)
        }
      })

      //generate random boolean for each vertex
      graph=graph.mapVertices[(Float,Boolean,Int,Int,Boolean)]((id,attr)=>(
        attr._1,random_num.nextBoolean,attr._3,attr._4,attr._5
      ))

      //determine which edge are in the set
      graph=graph.mapTriplets(edge=>{
        if (((edge.srcAttr._2==false && edge.dstAttr._2==true && (edge.srcAttr._3==edge.dstId) && (edge.dstAttr._4==edge.srcId))
          ||(edge.dstAttr._2==false && edge.srcAttr._2==true && (edge.dstAttr._3==edge.srcId) && (edge.srcAttr._4==edge.dstId)))
          && edge.attr._2==true && edge.srcAttr._5==true && edge.dstAttr._5==true){
          (edge.attr._1,true,true)
        } else {
          (edge.attr._1,edge.attr._2,edge.attr._3)
        }
      })


      //determine the neighbor vertex of the edges that are in the set
      local=graph.aggregateMessages[(Float,Boolean,Int,Int,Boolean)](msg=>{
        if (msg.attr._3==true && msg.srcAttr._5==true && msg.dstAttr._5==true){
          msg.sendToSrc(msg.srcAttr._1,msg.srcAttr._2,msg.srcAttr._3,msg.srcAttr._4,false)
          msg.sendToDst(msg.dstAttr._1,msg.dstAttr._2,msg.dstAttr._3,msg.dstAttr._4,false)
        } else{
          msg.sendToSrc(msg.srcAttr._1,msg.srcAttr._2,msg.srcAttr._3,msg.srcAttr._4,msg.srcAttr._5)
          msg.sendToDst(msg.dstAttr._1,msg.dstAttr._2,msg.dstAttr._3,msg.dstAttr._4,msg.dstAttr._5)
        }
      },(a,b)=>(
        if (a._5==false){
          (a._1,a._2,a._3,a._4,a._5)
        } else{
          (b._1,b._2,b._3,b._4,b._5)
        }
      ))
      //graph.join(local)
      graph=Graph(local,graph.edges)//,edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      //neighbor edges of those vertex that are connected to matched edge
      graph=graph.mapTriplets(edge=>{
        if (edge.srcAttr._5==false || edge.dstAttr._5==false){
          (edge.attr._1,true,edge.attr._3)
        } else{
          (edge.attr._1,edge.attr._2,edge.attr._3)
        }
      })

      //set edge to still active for those proposed edge that are not selected
      graph=graph.mapTriplets(edge=>{
        if((((edge.srcAttr._2==true || edge.dstAttr._2==false) && (edge.srcAttr._3==edge.dstId) && (edge.dstAttr._4==edge.srcId))
          || ((edge.dstAttr._2==true || edge.srcAttr._2==false) && (edge.dstAttr._3==edge.srcId) && (edge.srcAttr._4==edge.dstId)))
          && edge.attr._2==true && edge.srcAttr._5==true && edge.dstAttr._5==true){
          (edge.attr._1,false,edge.attr._3)
        } else{
          (edge.attr._1,edge.attr._2,edge.attr._3)
        }
      })
      active_edges=graph.edges.filter{case (edge)=>edge.attr._2==false}.count
      println(active_edges)
    }
    println("The alogrithms ran for "+ counter + " rounds.")
    val temp_output=graph.subgraph(epred=edge=>edge.attr._3==true).mapVertices[Int]((id,attr)=>1)
    val output=temp_output.mapEdges(edge=>1)

    return output
  }



  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("final_project").set("spark.driver.memory", "15g").set("spark.executor.memory","15g")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length != 2) {
        println("Please include input file path and output file path")
        sys.exit(1)
    }

    val startTimeMillis = System.currentTimeMillis()
    val edges = sc.textFile(args(0)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)}).filter{case Edge(a,b,c) => a!=b}
    val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
    val g2 = compute(g)

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("compute function completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.edges)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(1))
  }
}
