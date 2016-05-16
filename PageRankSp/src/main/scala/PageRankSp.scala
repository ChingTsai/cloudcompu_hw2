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

    val lines = sc.textFile(filePath, sc.defaultParallelism * 12)
    // lines.cache();

    val regex = "\\[\\[(.+?)([\\|#]|\\]\\])".r;

    var st = System.nanoTime

    var link =
      lines.map(line => {
        val lineXml = scala.xml.XML.loadString(line.toString())
        val title = (lineXml \ "title").text;

        val out = regex.findAllIn(lineXml.text).toArray
          .map { x => x.replaceAll("[\\[\\]]", "").split("[\\|#]") }
          .filter { _.length > 0 }.map(_.head.capitalize);

        out.map { x => (x, title) }.+:(title, "&gt")
      }).flatMap(y => y).groupByKey(sc.defaultParallelism * 10).filter(_._2.exists { _ == "&gt" })
        //
        .map(row => {
          row._2.toArray.filter(_ != "&gt").map(tp => (tp, row._1)).+:(row._1, "&gt");
        }).flatMap(y => y).groupByKey(sc.defaultParallelism * 10).map(x => (x._1, x._2.toArray.filter { _ != "&gt" }));

    link.cache();

    
    val n = link.count();
    val alpha = 0.85;

    var micros = (System.nanoTime - st) / 1000000000.0
    System.out.println("Parse :  %1.5f seconds".format(micros));

    //val res = link.map(x => (x._1, ":" + x._2.mkString(",")));
    //res.map(x => x._2.count)
    var rddPR = link.map(x => (x._1, (x._2, 1.0 / n)));

    var Err = 1.0;
    var iter = 0;

    rddPR.cache();
    //var tmpPR = rddPR.map(x => (x._1,x._2._2));

    while (Err > 0.001) {
      st = System.nanoTime

      val dangpr = rddPR.filter(_._2._1.length == 0).map(_._2._2).reduce(_ + _) / n * alpha;
      System.out.println("Dangl :  %1.5f ".format(dangpr));
      
      var tmpPR = rddPR.map(row => {

        row._2._1.map { tp => (tp, row._2._2 / row._2._1.length * alpha) }.+:(row._1, 1.0 / n * (1 - alpha) + dangpr);

      }).flatMap(y => y).reduceByKey(_ + _);
      Err = (tmpPR.join(rddPR.map(x => (x._1, x._2._2)), sc.defaultParallelism * 8)).map(x => (x._2._1 - x._2._2).abs).reduce(_ + _);
      rddPR = rddPR.map(x => (x._1, x._2._1)).join(tmpPR, sc.defaultParallelism * 8);

      micros = (System.nanoTime - st) / 1000000000.0
      System.out.println("Iteration : " + iter + " err: " + Err);
      System.out.println("Compute :  %1.5f seconds".format(micros))
      iter = iter + 1;
    }

    //res.saveAsTextFile(outputPath);
    val res = rddPR.map(x => (x._1, x._2._2));
    res.sortBy({ case (page, pr) => (-pr, page) }, true, sc.defaultParallelism * 10).map(x => x._1 + "\t" + x._2).saveAsTextFile(outputPath);

    sc.stop
  }
}

