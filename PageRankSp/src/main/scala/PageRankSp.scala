import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PageRankSp {
  def main(args: Array[String]) {
    val filePath = args(0)

    val outputPath = "Hw2/pageranksp"

    val conf = new SparkConf().setAppName("Page Rank Spark")
    val sc = new SparkContext(conf)

    // Cleanup output dir
    val hadoopConf = sc.hadoopConfiguration
    var hdfs = FileSystem.get(hadoopConf)
    try { hdfs.delete(new Path(outputPath), true) } catch { case _: Throwable => {} }

    val lines = sc.textFile(filePath, sc.defaultParallelism)

    val regex = "\\[\\[(.+?)([\\|#]|\\]\\])".r;
    var link =
        lines.map(line => {
        val title = (scala.xml.XML.loadString(line.toString()) \ "title").text;
        
        val out = regex.findAllIn(line).toList.map { x => x.replaceAll("[\\[\\]]","").split("[\\|#]").head };
        //val rddout = sc.parallelize(out, sc.defaultParallelism);
        (title.capitalize , out);
    });
    

    val linkMap = (link.map(x => x._1)).toArray().toSet;
   
    val bclinkMap = sc.broadcast(linkMap);
    link = link.map(l => {
      (l._1,l._2.filter { x => bclinkMap.value.contains(x) })
    })
    
    val res = link.map(x => (x._1,":"+x._2.mkString(",")));
    //res.map(x => x._2.count)
    val out_number = link.map(x => x._2.length).filter { _ ==0 }.count();
    System.out.println("Out target"+out_number);
    //res.saveAsTextFile(outputPath);
    res.sortBy(_._1.toString()).saveAsTextFile(outputPath);
    

    sc.stop
  }
}

