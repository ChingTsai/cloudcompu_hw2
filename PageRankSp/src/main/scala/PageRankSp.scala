import org.apache.spark._
import org.apache.hadoop.fs._
import java.net.URLDecoder

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

    // Read input file

    val lines = sc.textFile(filePath, sc.defaultParallelism)

    val regex = "<title>(.+?)</title>".r;
    val res = lines.map(line => {URLDecoder.decode(regex.findFirstIn(line).toString())});
    

    res.sortBy(_.toString()).saveAsTextFile(outputPath)

    sc.stop
  }
}

