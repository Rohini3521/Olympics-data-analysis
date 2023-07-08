import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object olympic {

  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("Oly")
      .getOrCreate()
    val schema = StructType(Array(
      StructField("Athlete", StringType, nullable = true),
      StructField("Age", IntegerType, nullable = true),
      StructField("Country", StringType, nullable = true),
      StructField("Year", IntegerType, nullable = true),
      StructField("ClosingDate", DateType, nullable = true),
      StructField("Sport", StringType, nullable = true),
      StructField("GoldMedals", IntegerType, nullable = true),
      StructField("SilverMedals", IntegerType, nullable = true),
      StructField("BronzeMedals", IntegerType, nullable = true),
      StructField("TotalMedals", IntegerType, nullable = true)
    )
    )
    val dateFormat = "MM/dd/yyyy"

    var df = spark.read
      .option("delimiter",",")
      .schema(schema)
      .option("dateFormat", dateFormat)
      .csv("D:/Olympic/input2.csv")

    df.printSchema()
    // println("****************** 50 *****************")
    // df.show(50)
    println("total count  " +df.count())
    val datanull = df.filter(col("Age").isNull).show()
    //println(datanull.count())
    df = df.drop(col("ClosingDate"))
    println(" Closing Date Drop")
    df.show()
    df = df.na.drop()
    println(" All na drop")
    df.printSchema()
    df.show()
    println(df.count())

    println("Q1")
    val dist=df.select("Sport").distinct()
    println("Dist :" + dist.count())

    println("Q2")
    val totalMedalsDF = df.groupBy("Sport").agg(sum("TotalMedals").alias("TotalMedals"))
      .orderBy(desc("TotalMedals")).limit(10)
    totalMedalsDF.show()

    println("Q3")
    val topc = df.groupBy("Country").agg(sum("TotalMedals").alias("TotalMedals"))
      .orderBy(desc("TotalMedals")).limit(10)

    println("Q4")
    val topa = df.groupBy("Athlete").agg(sum("TotalMedals").alias("TotalMedals"))
      .orderBy(desc("TotalMedals")).limit(10)

    println("Q5")
    val topc12 = df.filter(col("Year")==="2012").groupBy("Country")
      .agg(sum("GoldMedals").alias("GoldMedals"))
      .orderBy(desc("GoldMedals")).first()
    println(topc12)

    println("Q6")
    val oldest = df.filter(col("Sport")==="Gymnastics" && col("GoldMedals").notEqual(0))
      .orderBy(desc("Age")).select("Athlete").first()
    println(oldest)

    println("Q7")
    val duplicateAthletesDF = df.groupBy("Athlete", "Sport").count()
      .select("Athlete", "Sport","count")
    //duplicateAthletesDF.coalesce(1).write.csv("D:/dup.csv")

    val ath = duplicateAthletesDF.groupBy("Athlete").count()
      .filter(col("count") > 1).select("Athlete","count")
      .show(50)

    println(ath)


    println("Q8")
    val teen = df.filter(col("Age")>=13 && col("Age") <= 19).select("Athlete").distinct().count()
    println(teen)

    println("Q9")
    val correlation = df.select(corr("Age", "TotalMedals").alias("correlation")).show()

    println("Q10")

    val title = df.withColumn("title",when(col("GoldMedals") >=4,"Legend")
      .when(col("GoldMedals") >=1,"Pro").otherwise("Aspirant"))
      .groupBy(col("title")).count().show()

    println("Q11")

    val lew = df.filter(col("Athlete").like("%Lewis%")).select("Athlete").distinct().show()


    println("Q12")
    val agg = df.filter(col("TotalMedals") >= 1)
      .agg(avg("Age").as("avgAge"),min("Age").as("minAge")
        ,max("Age").as("maxAge")).show()

    println("Q13")

    val filteredDF = df.filter(col("Country") === "United States" && col("Year")
      .isin(2000, 2002, 2004, 2006, 2008, 2010, 2012))

    var resultDF = filteredDF.groupBy("Country","Year")
      .agg(
        round(sum("GoldMedals"),2).as("GoldMedals"),
        round(sum("SilverMedals"),2).as("SilverMedals"),
        round(sum("BronzeMedals"),2).as("BronzeMedals"),
        round(sum("TotalMedals"),2).as("TotalMedals")
      )
      .orderBy("Year")


    println("Q14")
    var per = resultDF.withColumn("PercentGold",round(col("GoldMedals")/col("TotalMedals")*100))
      .withColumn("PercentSilver",round(col("SilverMedals")/col("TotalMedals")*100))
      .withColumn("PercentBronze",round(col("BronzeMedals")/col("TotalMedals")*100))
      .select("Country","Year","PercentGold","PercentSilver","PercentBronze").show()

    println("Q15")

    val dfWithAgeCat = df.withColumn("ageCat", when(col("Age").between(13, 19), "teens")
      .when(col("Age").between(20, 29), "twenties")
      .when(col("Age").between(30, 39), "thirties")
      .when(col("Age").between(40, 49), "fourties")
      .when(col("Age").between(50, 59), "fifties")
      .when(col("Age").between(60, 69), "sixties")
      .otherwise("Unknown"))
    var dfshow = dfWithAgeCat.groupBy("ageCat")
      .agg(sum("TotalMedals").alias("TotalMedals"))select("ageCat","TotalMedals")
    dfshow.show()
    // Calculate the total number of medals won in each age category
    /* val medalsByAgeCat = dfWithAgeCat.groupBy("ageCat")
       .agg(sum("GoldMedals").alias("TotalGoldMedals"),
         sum("SilverMedals").alias("TotalSilverMedals"),
         sum("BronzeMedals").alias("TotalBronzeMedals"))
       .orderBy("TotalGoldMedals", "TotalSilverMedals", "TotalBronzeMedals")
     dfWithAgeCat.show()*/

    println("Q16")
    val avgAgeDF = dfWithAgeCat.groupBy("Sport")
      .agg(avg("Age").cast("Int").alias("avgAge"))
      .orderBy("avgAge", "Sport")
    println(avgAgeDF.count())
    avgAgeDF.show()

    println("Q17")

    val swimmingDF = df.filter(col("Sport") === "Swimming").groupBy("Sport","Country")
      .agg(avg("Age").cast("Int").alias("avgAge"))
      .orderBy("avgAge", "Country")

    swimmingDF.show()
    println(swimmingDF.count())

    println("Q18")
    //      val litt = df.filter(col("GoldMedals").notEqual(0))
    //        .withColumn("teens",when(col("Age").between(13,19),lit(1)))
    //        .withColumn("twenties",when(col("Age").between(20,29),lit(1)))
    //        .withColumn("thirties",when(col("Age").between(30,39),lit(1)))
    //        .withColumn("fourties",when(col("Age").between(40,49),lit(1)))
    //        .withColumn("fifties",when(col("Age").between(50,59),lit(1)))
    //        .withColumn("sixties",when(col("Age").between(60,69),lit(1)))
    //        .groupBy(col("Country")).agg(sum("teens").as("teens"),sum("twenties").as("twenties"),
    //        sum("thirties").as("thirties"),sum("fourties").as("fourties"),sum("fifties").as("fifties"),
    //        sum("sixties").as("sixties")).select("Country","teens","twenties","thirties","fourties","fifties","sixties").show()
    //      //litt.printSchema()

    // Calculate the total number of gold medals awarded to each country for each age category
    // Calculate the total number of gold medals awarded to each country for each age category
    val goldMedalsByAgeCatAndCountry = dfWithAgeCat.groupBy("Country")
      //.pivot("ageCat", Seq("teens", "twenties", "thirties", "forties", "fifties", "sixties"))
      .agg(sum("GoldMedals").as("Total"))
    // .orderBy(col("Total").desc)
    goldMedalsByAgeCatAndCountry.show()

    // Take the top 10 results
    val top10GoldMedalsByAgeCatAndCountry = goldMedalsByAgeCatAndCountry.limit(10)

    // Add a 'Total' column as the sum of all age categories
    val finalResult = top10GoldMedalsByAgeCatAndCountry.withColumn("Total",
      col("teens") + col("twenties") + col("thirties") + col("forties") + col("fifties") + col("sixties"))


    finalResult.show()


    println("Q19")
    val goldMedalsByAgeCatAndSport = dfWithAgeCat.groupBy("Sport")
      .pivot("ageCat", Seq("teens", "twenties", "thirties", "forties", "fifties"))
      .agg(sum("GoldMedals").as("Total"))limit(10)


  }
}
