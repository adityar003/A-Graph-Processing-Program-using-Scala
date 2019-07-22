import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.math.min


@SerialVersionUID(123L)
case class Objct1(aNode:Long, aGrp:Long, bNode:Long, bGrp:Long) extends Serializable{}

  object Graph{
    def main(args: Array[String]){
      val conf = new SparkConf().setAppName("Assignment_5")
      val sc = new SparkContext(conf)
      
      /*var graph=sc.textFile(args(0)).map(line=>{val a = line.split(",") var elem=new ListBuffer[Long]()
        for(i <- 1 to (a.length))
        {
          elem=a(i).toLong  //.map(_.toLong)
        }
        var adj=elem.toList(a(0).toLong,a(0).toLong,adj)
      })
	*/
      
      
       var graph = sc.textFile(args(0)).map( line => { val a = line.split(",")//.map(_.toLong)
                                                      (a(0).toLong,a(0).toLong,a.drop(1).toList.map(_.toString).map(_.toLong)) })  
                                                      
       //graph.foreach(println)
      var graph1 = graph.map(g => (g._1,g))
      
      graph1.foreach(println)
      
      for(i <- 1 to 5){

         graph = graph.flatMap(
         map => map match{ case (x, y, xy) => (x, y) :: xy.map(z => (z, y) ) } )
        .reduceByKey((a, b) => (if (a >= b) b else a))
        .join(graph1).map(g => (g._2._2._2, g._2._1, g._2._2._3))
        //.collect()
    }
 val groupCount = graph.map(g => (g._2, 1))
 .reduceByKey((m, n) => (m + n))
 .collect()
 .foreach(println)
 }
}