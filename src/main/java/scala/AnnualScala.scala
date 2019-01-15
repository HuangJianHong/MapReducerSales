package scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


//定义一个case class来代表订单表
//指定DataFrame的schema
case class OrderInfo(year: Int, amount: Double)

/**
  * 使用SparkSql进行分析处理
  */
object AnnualTotal {

  //TODO 需要将resources目录下的的 log4j.properties 移除掉，避免过多的日志影响

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //数据源：13,987,1998-01-10,3,999,1,1232.16
    val conf: SparkConf = new SparkConf().setAppName("AnnualTotal").setMaster("local")

    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)

    import sqlContext.implicits._ //记得导入包
    val data: RDD[String] = sparkContext.textFile("F:\\Tmp\\sales")

    val orderDF = data.map(line => {
      val strings: Array[String] = line.split(",")
      val year: String = strings(2).substring(0, 4)
      val money: String = strings(6)

      (Integer.parseInt(year), money.toDouble)
      //  val words = line.split(",")
      //  取出 年份和金额
      // (Integer.parseInt(words(2).substring(0,4)),Double.parseDouble(words(6)))
    }).map(order => OrderInfo(order._1, order._2)).toDF()

    //查看表结构
    orderDF.printSchema();
    /**
      * root
      * |-- year: integer (nullable = true)
      * |-- amount: double (nullable = true)
      */

    //创建视图
    orderDF.createOrReplaceTempView("orders")

    //执行查询  1、SparkSession.sql()   2、sqlContext.sql()
    sqlContext.sql("select year, count(amount), sum(amount) from orders group by year").show()

    sparkContext.stop()

    /**
      * 最终输出结果：
      * +----+-------------+--------------------+
      * |year|count(amount)|         sum(amount)|
      * +----+-------------+--------------------+
      * |1998|       178834| 2.408391494998051E7|
      * |2001|       259418|2.8136461979984347E7|
      * |2000|       232646|2.3765506619993567E7|
      * |1999|       247945|2.2219947660012607E7|
      * +----+-------------+--------------------+
      */
  }

}
