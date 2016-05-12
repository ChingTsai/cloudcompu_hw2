import org.apache.spark._
import org.apache.hadoop.fs._

object PageRankSp {
  def main(args: Array[String]) {
    val filePath = args(0)
    val name = args(1)
    val iters = args(2).toInt

    val outputPath = "Hw2/pageranksp"

    val conf = new SparkConf().setAppName("Page Rank Spark")
    val sc = new SparkContext(conf)

    // Cleanup output dir
    val hadoopConf = sc.hadoopConfiguration
    var hdfs = FileSystem.get(hadoopConf)
    try { hdfs.delete(new Path(outputPath), true) } catch { case _: Throwable => {} }

    // Read input file
    def f(s: String) = List(s.split(",")(1) -> s.split(",")(0), s.split(",")(0) -> s.split(",")(1));
    val lines = sc.textFile(filePath, sc.defaultParallelism)
    var map = lines.flatMap(x => f(x));
    //println(res);
    // Step 1: initial friend list, map each line in file to pairs
    // ex: "Tom,Alice" => ("Tom" -> "Alice"), ("Alice" -> "Tom")
    // hint: use map() and union()
    //       Or, you can use flatMap() to do it at once
    // ...
    
    
    // Step 2: make an RDD from 'name'
    // hint: use sc.parallelize()
    // optional: you can pass (sc.defaultParallelism * 3) as # of slices for better performance
    //
    // var res = ...
    var res = List(name);
    var rdd = sc.parallelize(res,sc.defaultParallelism*3);
    // Step 3: use initial result and friend list to find all friends within 'iters' hops
    // hint: use join(), union() and distinct()
    // optional: resize numPartitions to (sc.defaultParallelism * 3) at the end of each iteration
    //
    
    for (i <- 1 to iters) {
      val tpl = rdd.map { x => (x,x) };
      val tmp_map = tpl.join(map);
      rdd = rdd ++ tmp_map.values.values;
      rdd = rdd.distinct();
      //val friend = map.filter(x => rdd.collect().contains(x._1)).values.distinct();
      
      
    }

    // Action branch! Add cache() to avoid re-computation
    //res = res.cache
    rdd =  rdd.cache();
    // Output: count() and saveAsTextFile()
    //println("Count := " + res.count)
    rdd.sortBy(_.toString).saveAsTextFile(outputPath)

    sc.stop
  }
}

