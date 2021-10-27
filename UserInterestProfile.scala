import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import java.text.SimpleDateFormat
import scala.collection.mutable.{Map, ArrayBuffer}
import util.Random
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import java.util.Calendar


object UserInterestProfile {
  def regular(tem:List[String])={

      val result=tem.mkString(",").split(",").map(x=>x.split(":")
      ).filter(x=>x.size==2).map(x=>(x(0),x(1))).filter(x=> isIntByRegex(x._2)
      ).groupBy(x=>x._1).map(x=>(x._1,x._2.map(x=>x._2.toInt).sum)
      ).toList.sortBy(x=> -x._2).map(x=> x._1+";"+x._2).toArray

      result
   }
  def isIntByRegex(s : String) = {
      val pattern = """^(\d+)$""".r
      s match {
        case pattern(_*) => true
        case _ => false
      }
   }
  def main(arguments: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("GenSkuSessionProfile")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    println("@@enter GenSkuSessionProfile")
    val sc = spark.sparkContext
    val sqlContext = new SQLContext(sc)
    val args = new CommonArgs(arguments)

    val date = new SimpleDateFormat("yyyy-MM-dd").format(args.date)
    var cur_cal = Calendar.getInstance
    cur_cal.setTime(args.date)
    cur_cal.add(Calendar.DATE, -7)
    val deldate = new SimpleDateFormat("yyyy-MM-dd").format(cur_cal.getTime)
    println(deldate)

    val path="hdfs://ns1017/user/jd_ad/ads_bgn/ad_bgn.db/user_embedding/user_sku_interest_daily/"
    //val part="/part-00001"
    val num_short = 30
    var path_set_short = collection.mutable.Set[String]()
    for (i <- 0 to num_short){
      var cal = Calendar.getInstance
      cal.setTime(args.date)
      cal.add(Calendar.DATE, -i)
      val startdate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      path_set_short.add(path+startdate)
    }
    var user_interest_path_short = path_set_short.toList.mkString(",")

    val num_mid = 90
    var path_set_mid = collection.mutable.Set[String]()
    for (i <- 0 to num_mid){
      var cal = Calendar.getInstance
      cal.setTime(args.date)
      cal.add(Calendar.DATE, -i)
      val startdate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      path_set_mid.add(path+startdate)
    }
    var user_interest_path_mid = path_set_mid.toList.mkString(",")

    val num_long = 365
    var path_set_long = collection.mutable.Set[String]()
    for (i <- 0 to num_long){
      var cal = Calendar.getInstance
      cal.setTime(args.date)
      cal.add(Calendar.DATE, -i)
      val startdate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      path_set_long.add(path+startdate)
    }
    var user_interest_path_long = path_set_long.toList.mkString(",")


    val df_short = sc.textFile(user_interest_path_short).map{x=>x.split("\t")
    }.filter(x=>x.size==9).map{x=>
      val device_id=x(0)
      val user_pin=x(1)
      val brand_id=x(2)
      val cid1=x(3)
      val cid2=x(4)
      val cid3=x(5)
      val blc=x(6)
      val product=x(7)
      val search_word=x(8)
      (user_pin ,( brand_id,cid1,cid2,cid3,blc,product,search_word ) )

    }.groupByKey().map { x =>
      // val device_id=x._1._1
      val user_pin = x._1
      val result = x._2.toList
      println(result.mkString(","))
      val brand_id = regular(result.map(x => x._1))
      val cid1 = regular(result.map(x => x._2))
      val cid2 = regular(result.map(x => x._3))
      val cid3 = regular(result.map(x => x._4))
      val blc = regular(result.map(x => x._5))
      val product = regular(result.map(x => x._6))
      val search_word = regular(result.map(x => x._7))

      (user_pin, (cid1, cid2, cid3, product, blc, brand_id, search_word))
    }
      /*.toDF("user_pin", "short_cid1_interest_tag", "short_cid2_interest_tag", "short_cid3_interest_tag",
      "short_blc_interest_tag", "short_product_interest_tag", "short_brand_interest_tag", "short_search_word_tag")*/
    val df_mid = sc.textFile(user_interest_path_mid).map{x=>x.split("\t")
    }.filter(x=>x.size==9).map{x=>
      val device_id=x(0)
      val user_pin=x(1)
      val brand_id=x(2)
      val cid1=x(3)
      val cid2=x(4)
      val cid3=x(5)
      val blc=x(6)
      val product=x(7)
      val search_word=x(8)
      (user_pin, (brand_id,cid1,cid2,cid3,blc,product,search_word))

    }.groupByKey().map { x =>
      // val device_id=x._1._1
      val user_pin = x._1
      val result = x._2.toList
      println(result.mkString(","))
      val brand_id = regular(result.map(x => x._1))
      val cid1 = regular(result.map(x => x._2))
      val cid2 = regular(result.map(x => x._3))
      val cid3 = regular(result.map(x => x._4))
      val blc = regular(result.map(x => x._5))
      val product = regular(result.map(x => x._6))
      val search_word = regular(result.map(x => x._7))

      (user_pin, (cid1, cid2, cid3, product, blc, brand_id, search_word))
    }

    val df_long = sc.textFile(user_interest_path_long).map{x=>x.split("\t")
    }.filter(x=>x.size==9).map{x=>
      val device_id=x(0)
      val user_pin=x(1)
      val brand_id=x(2)
      val cid1=x(3)
      val cid2=x(4)
      val cid3=x(5)
      val blc=x(6)
      val product=x(7)
      val search_word=x(8)
      (user_pin, (brand_id,cid1,cid2,cid3,blc,product,search_word))
    }.groupByKey().map { x =>
      val user_pin = x._1
      val result = x._2.toList
      println(result.mkString(","))
      val brand_id = regular(result.map(x => x._1))
      val cid1 = regular(result.map(x => x._2))
      val cid2 = regular(result.map(x => x._3))
      val cid3 = regular(result.map(x => x._4))
      val blc = regular(result.map(x => x._5))
      val product = regular(result.map(x => x._6))
      val search_word = regular(result.map(x => x._7))

      (user_pin, (cid1, cid2, cid3, product, blc, brand_id, search_word))
    }

    val final_df = df_long.leftOuterJoin(df_mid)
      .map {
        x =>
          var s1 = Array("-1")
          var s2 = Array("-1")
          var s3 = Array("-1")
          var s4 = Array("-1")
          var s5 = Array("-1")
          var s6 = Array("-1")
          var s7 = Array("-1")
          if(x._2._2 != None){
            val info = x._2._2.toList(0)
            s1 = info._1
            s2 = info._2
            s3 = info._3
            s4 = info._4
            s5 = info._5
            s6 = info._6
            s7 = info._7
          }
          (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._1._6, x._2._1._7,
            s1, s2, s3, s4, s5, s6, s7)
          )

      }
      .leftOuterJoin(df_short).map{
      x =>
        var x1 = Array("-1")
        var x2 = Array("-1")
        var x3 = Array("-1")
        var x4 = Array("-1")
        var x5 = Array("-1")
        var x6 = Array("-1")
        var x7 = Array("-1")
        if(x._2._2 != None){
          val info1 = x._2._2.toList(0)
          x1 = info1._1
          x2 = info1._2
          x3 = info1._3
          x4 = info1._4
          x5 = info1._5
          x6 = info1._6
          x7 = info1._7
        }
        (x._1, x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._1._6, x._2._1._7,
          x._2._1._8, x._2._1._9, x._2._1._10, x._2._1._11, x._2._1._12, x._2._1._13, x._2._1._14,
          x1, x2, x3, x4, x5, x6, x7)

    }
    sqlContext.sql(s"alter table app.kg_user_interest_tag drop  partition(dt='${date}')")
    sqlContext.sql(s"alter table app.kg_user_interest_tag drop  partition(dt='${deldate}')")
    final_df.repartition(1000).toDF().write.format("orc").save("hdfs://ns1017/user/ads_bgn/kg_user_interest_tag/dt=%s".format(date))
    sqlContext.sql("msck repair table app.kg_user_interest_tag")
    //sqlContext.sql(s"use ad_bgn")
    //df.write.saveAsTable(s"user_interest_short_table_"+date)*/
  }
}