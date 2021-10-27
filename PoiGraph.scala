import java.util.Calendar
import java.util.Date
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import java.text.SimpleDateFormat

class HiveData{
   def getLogDF(sc: SparkContext): DataFrame = {
       val hiveContext = new HiveContext(sc)
      /* var query = s"""
select xtl_bpin2cpin.bpin,ad_dmp_user_profile.age,count(*) as age_cnt from ads_bgn_exp1.xtl_bpin2cpin,ad.ad_dmp_user_profile where xtl_bpin2cpin.device_id = ad_dmp_user_profile.user_pin group by xtl_bpin2cpin.bpin,ad_dmp_user_profile.age"""*/
       var query = s"""
      select * from ad_bgn.bpin_user
      """
       hiveContext.sql(query)
   }
}

object PoiGraph {
    def main(args: Array[String]): Unit = {
       var cal = Calendar.getInstance
       cal.add(Calendar.DATE, -1)
       var date: Date = cal.getTime
       val newDate = new SimpleDateFormat("yyyy-MM-dd").format(date)
       val separator = ":"
       val spark = SparkSession
       .builder()
       .appName("PoiGraphBuild")
       .enableHiveSupport()
       .getOrCreate()
       import spark.implicits._
       val sc = spark.sparkContext

       val sqlContext = new HiveContext(sc)
       sqlContext.sql(s"use ad_bgn")
       sqlContext.sql(s"truncate table bpin_user")
       sqlContext.sql(
         s"""insert into ad_bgn.bpin_user select bpin, age.value[0], gender.value[0], user_education.value[0],
            |user_profession.value[0], purchasing_power.value[0], marriage_status.value[0], has_child.value[0],
            |children_birth.value[0], child_age_range.value[0], child_gender_ad.value[0], pet_owner.value[0],
            |car_owners.value[0], user_category_preference.value[0], family_owners.value[0], promotions_sensitive_level.value[0],
            |user_average_order_amount_in_6_months.value[0] from ads_bgn_exp1.xtl_bpin2cpin, ad.ad_dmp_user_profile,
            |app.app_user_mapping_raw  where xtl_bpin2cpin.device_id=app_user_mapping_raw.device_id and
            |app_user_mapping_raw.pin = ad_dmp_user_profile.user_pin and ad_dmp_user_profile.dt='${newDate}' and
            |app_user_mapping_raw.dt='${newDate}' and xtl_bpin2cpin.dt = '${newDate}'""".stripMargin)
       /*select("xtl_bpin2cpin.bpin", "ad_dmp_user_profile.age","age_cnt")
       var outPath = "hdfs://ns1018/user/jd_ad/ads_bgn/liuzesheng/age"
       hiveData.rdd.saveAsTextFile(outPath)
       println(hiveData.collect.foreach(println))*/
       val hiveData = (new HiveData).getLogDF(sc)
       hiveData.select("bpin").distinct().show()
       //年龄
       val ageData = hiveData.select("bpin", "age").groupBy("bpin", "age").count()
       .withColumnRenamed("count","age_count")
       val sageData = ageData.select($"bpin", concat_ws(separator, $"age", $"age_count")
       .cast(StringType).as("age_count_map")).groupBy("bpin")
       .agg(concat_ws(",", collect_list("age_count_map")) as "age_count_map")
       //性别
       val genderData = hiveData.select("bpin", "gender").groupBy("bpin", "gender").count()
       .withColumnRenamed("count","gender_count")
       val sgenderData = genderData.select($"bpin", concat_ws(separator, $"gender", $"gender_count")
       .cast(StringType).as("gender_count_map")).groupBy("bpin")
       .agg(concat_ws(",", collect_list("gender_count_map")) as "gender_count_map")
       //教育
       val eduData = hiveData.select("bpin", "user_education").groupBy("bpin", "user_education")
       .count().withColumnRenamed("count","edu_count")
       val seduData = eduData.select($"bpin", concat_ws(separator, $"user_education", $"edu_count")
       .cast(StringType).as("edu_count_map")).groupBy("bpin")
       .agg(concat_ws(",", collect_list("edu_count_map")) as "edu_count_map")
       //职业
       val jobData = hiveData.select("bpin", "user_profession").groupBy("bpin", "user_profession")
         .count().withColumnRenamed("count","job_count")
       val sjobData = jobData.select($"bpin", concat_ws(separator, $"user_profession", $"job_count")
         .cast(StringType).as("job_count_map")).groupBy("bpin")
         .agg(concat_ws(",", collect_list("job_count_map")) as "job_count_map")
       //购买力
       val buyData = hiveData.select("bpin", "purchasing_power").groupBy("bpin", "purchasing_power")
         .count().withColumnRenamed("count","buy_count")
       val sbuyData = buyData.select($"bpin", concat_ws(separator, $"purchasing_power", $"buy_count")
        .cast(StringType).as("buy_count_map")).groupBy("bpin")
        .agg(concat_ws(",", collect_list("buy_count_map")) as "buy_count_map")
       //婚姻
       val hyData = hiveData.select("bpin", "marriage_status").groupBy("bpin", "marriage_status")
         .count().withColumnRenamed("count","hy_count")
       val shyData = hyData.select($"bpin", concat_ws(separator, $"marriage_status", $"hy_count")
        .cast(StringType).as("hy_count_map")).groupBy("bpin")
        .agg(concat_ws(",", collect_list("hy_count_map")) as "hy_count_map")
       //是否有小孩
       val hasData = hiveData.select("bpin", "has_child").groupBy("bpin", "has_child")
         .count().withColumnRenamed("count","has_count")
       val shasData = hasData.select($"bpin", concat_ws(separator, $"has_child", $"has_count")
        .cast(StringType).as("has_count_map")).groupBy("bpin")
        .agg(concat_ws(",", collect_list("has_count_map")) as "has_count_map")
       //孩子年龄
       val hnData = hiveData.select("bpin", "child_age_range").groupBy("bpin", "child_age_range")
         .count().withColumnRenamed("count","hn_count")
       val shnData = hnData.select($"bpin", concat_ws(separator, $"child_age_range", $"hn_count")
        .cast(StringType).as("hn_count_map")).groupBy("bpin")
        .agg(concat_ws(",", collect_list("hn_count_map")) as "hn_count_map")
       //孕龄
       val ynData = hiveData.select("bpin", "children_birth").groupBy("bpin", "children_birth")
         .count().withColumnRenamed("count","yn_count")
       val synData = ynData.select($"bpin", concat_ws(separator, $"children_birth", $"yn_count")
        .cast(StringType).as("yn_count_map")).groupBy("bpin")
        .agg(concat_ws(",", collect_list("yn_count_map")) as "yn_count_map")
       //孩子性别
       val cgenderData = hiveData.select("bpin", "child_gender_ad").groupBy("bpin", "child_gender_ad")
         .count().withColumnRenamed("count","cgender_count")
       val scgenderData = cgenderData.select($"bpin", concat_ws(separator, $"child_gender_ad", $"cgender_count")
        .cast(StringType).as("cgender_count_map")).groupBy("bpin")
        .agg(concat_ws(",", collect_list("cgender_count_map")) as "cgender_count_map")
       //是否有宠物
       val petData = hiveData.select("bpin", "pet_owner").groupBy("bpin", "pet_owner")
         .count().withColumnRenamed("count","pet_count")
       val spetData = petData.select($"bpin", concat_ws(separator, $"pet_owner", $"pet_count")
        .cast(StringType).as("pet_count_map")).groupBy("bpin")
        .agg(concat_ws(",", collect_list("pet_count_map")) as "pet_count_map")
       //是否有车
       val carData = hiveData.select("bpin", "car_owners").groupBy("bpin", "car_owners")
         .count().withColumnRenamed("count","car_count")
       val scarData = carData.select($"bpin", concat_ws(separator, $"car_owners", $"car_count")
        .cast(StringType).as("car_count_map")).groupBy("bpin")
        .agg(concat_ws(",", collect_list("car_count_map")) as "car_count_map")
       //品类偏好
       val kindData = hiveData.select("bpin", "user_category_preference").groupBy("bpin", "user_category_preference")
         .count().withColumnRenamed("count","kind_count")
       val skindData = kindData.select($"bpin", concat_ws(separator, $"user_category_preference", $"kind_count")
        .cast(StringType).as("kind_count_map")).groupBy("bpin")
        .agg(concat_ws(",", collect_list("kind_count_map")) as "kind_count_map")
       //是否有房
       val houseData = hiveData.select("bpin", "family_owners").groupBy("bpin", "family_owners")
         .count().withColumnRenamed("count","house_count")
       val shouseData = houseData.select($"bpin", concat_ws(separator, $"family_owners", $"house_count")
        .cast(StringType).as("house_count_map")).groupBy("bpin")
        .agg(concat_ws(",", collect_list("house_count_map")) as "house_count_map")
       //促销敏感度
       val sensitiveData = hiveData.select("bpin", "promotions_sensitive_level").groupBy("bpin", "promotions_sensitive_level")
         .count().withColumnRenamed("count","sensitive_count")
       val ssensitiveData = sensitiveData.select($"bpin", concat_ws(separator, $"promotions_sensitive_level", $"sensitive_count")
        .cast(StringType).as("sensitive_count_map")).groupBy("bpin")
        .agg(concat_ws(",", collect_list("sensitive_count_map")) as "sensitive_count_map")
       //用户月平均支付总金额
       val payData = hiveData.select("bpin", "user_average_order_amount_in_6_months").groupBy("bpin", "user_average_order_amount_in_6_months")
         .count().withColumnRenamed("count","pay_count")
       val spayData = payData.select($"bpin", concat_ws(separator, $"user_average_order_amount_in_6_months", $"pay_count")
        .cast(StringType).as("pay_count_map")).groupBy("bpin")
        .agg(concat_ws(",", collect_list("pay_count_map")) as "pay_count_map")
       //合并
       val resData = sageData.join(sgenderData, Seq("bpin"))
           .join(seduData, Seq("bpin"))
           .join(sjobData, Seq("bpin"))
           .join(sbuyData, Seq("bpin"))
           .join(shyData, Seq("bpin"))
           .join(shasData, Seq("bpin"))
           .join(shnData, Seq("bpin"))
           .join(synData, Seq("bpin"))
           .join(scgenderData, Seq("bpin"))
           .join(spetData, Seq("bpin"))
           .join(scarData, Seq("bpin"))
           .join(skindData, Seq("bpin"))
           .join(shouseData, Seq("bpin"))
           .join(ssensitiveData, Seq("bpin"))
           .join(spayData, Seq("bpin"))
       sageData.show()
       sgenderData.show()
       seduData.show()
       resData.show()
       resData.createOrReplaceTempView("mytempTable")
       sqlContext.sql(s"create table if not exists xtl_poi_graph(" +
         s"bpin string," +
         s"age_count_map string," +
         s"gender_count_map string," +
         s"edu_count_map string," +
         s"job_count_map string,"+
         s"buy_count_map string,"+
         s"hy_count_map string,"+
         s"has_count_map string,"+
         s"hn_count_map string,"+
         s"yn_count_map string,"+
         s"cgender_count_map string,"+
         s"pet_count_map string,"+
         s"car_count_map string,"+
         s"kind_count_map string,"+
         s"house_count_map string,"+
         s"sensitive_count_map string,"+
         s"pay_count_map string"+
         s") partitioned by (dt string)")
       sqlContext.sql(s"alter table xtl_poi_graph add partition(dt='${newDate}')")
       sqlContext.sql(s"insert into xtl_poi_graph partition(dt='${newDate}') select * from mytempTable")
       //val res = resData.rdd.filter(x=>x.getString(1).contains(",")).take(10).foreach { println }
    }
}