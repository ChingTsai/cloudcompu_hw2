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
    val res = lines.map(line => {
        val title = (scala.xml.XML.loadString(line.toString()) \ "title").text;
        (title ,regex.findAllIn(line).subgroups.map { x => x.substring(2, x.length()-2) }.mkString);
    });

    res.sortBy(_._1.toString()).saveAsTextFile(outputPath)

    sc.stop
  }
}

