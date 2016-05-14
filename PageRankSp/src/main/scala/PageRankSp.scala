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

    val lines = sc.textFile(filePath, sc.defaultParallelism * 10)
    lines.cache();

    val regex = "\\[\\[(.+?)([\\|#]|\\]\\])".r;

    var st = System.nanoTime

    var link =
      lines.map(line => {
        val lineXml = scala.xml.XML.loadString(line.toString())
        val title = (lineXml \ "title").text;

        val out = regex.findAllIn(lineXml.text).toList
          .map { x => x.replaceAll("[\\[\\]]", "").split("[\\|#]") }
          .filter { _.length > 0 }.map(_.head.capitalize);
        //val rddout = sc.parallelize(out, sc.defaultParallelism);
        (title.capitalize, out);
      });

    val linkMap = (link.map(x => x._1)).toArray().toSet;

    val bclinkMap = sc.broadcast(linkMap);
    //remove missing link
    link = link.map(l => {
      (l._1, l._2.filter { x => bclinkMap.value.contains(x) })
    })

    link.cache();

    val m = link.map(x => x._2.length).filter { _ == 0 }.count();
    val n = bclinkMap.value.size;
    val alpha = 0.85;

    var micros = (System.nanoTime - st) / 1000000000
    println("Parse :  %f seconds".format(micros))

    //val res = link.map(x => (x._1, ":" + x._2.mkString(",")));
    //res.map(x => x._2.count)
    var rddPR = link.map(x => (x._1, (x._2.toArray, 1.0 / n)));

    var Err = 1.0;
    var iter = 0;

    rddPR.cache();
    //var tmpPR = rddPR.map(x => (x._1,x._2._2));

    while (Err > 0.001) {
      st = System.nanoTime

      val dangpr = rddPR.filter(_._2._1.length == 0).map(_._2._2).reduce(_ + _) / n * alpha;
      var tmpPR = rddPR.map(row => {

        row._2._1.map { tp => (tp, row._2._2 / row._2._1.length * alpha) } ++ Array((row._1, 1.0 / n * (1 - alpha) + dangpr));

      }).flatMap(y => y).reduceByKey(_ + _);
      Err = (tmpPR.join(rddPR.map(x => (x._1, x._2._2)))).map(x => (x._2._1 - x._2._2).abs).reduce(_ + _);
      rddPR = rddPR.map(x => (x._1, x._2._1)).join(tmpPR, sc.defaultParallelism * 10);

      micros = (System.nanoTime - st) / 1000000000
      System.out.println("Iteration : " + iter + " err: " + Err);
      println("Compute :  %f seconds".format(micros))
      iter = iter + 1;
    }

    //res.saveAsTextFile(outputPath);
    val res = rddPR.map(x => (x._1, x._2._2));
    res.sortBy({ case (page, pr) => (-pr, page) }, true, sc.defaultParallelism * 3).map(x => x._1 + "\t" + x._2).saveAsTextFile(outputPath);

    sc.stop
  }
}

