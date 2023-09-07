import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.example.Schema.getSchema

object olympic {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("Oly")
      .getOrCreate()
    val schema = getSchema
    val dateFormat = "MM/dd/yyyy"

    var df = spark.read
      .option("delimiter", ",")
      .schema(schema)
      .option("dateFormat", dateFormat)
      .csv("D:/Olympic/input2.csv")

    // filter data where age is null
    val age_null = df.filter(col("Age").isNull).show()

    //drop column "ClosingDate"
    df = df.drop(col("ClosingDate"))

    // drop all null values
    df = df.na.drop()

    println("Q1")
    val dist = df.select("Sport").distinct()
    println("Distinct Count :" + dist.count())

    println("Q2")
    val topSport = df.groupBy("Sport").agg(sum("TotalMedals").alias("TotalMedals"))
      .orderBy(desc("TotalMedals")).limit(10)

    println("Q3")
    val topCountry = df.groupBy("Country").agg(sum("TotalMedals").alias("TotalMedals"))
      .orderBy(desc("TotalMedals")).limit(10)

    println("Q4")
    val topAthlete = df.groupBy("Athlete").agg(sum("TotalMedals").alias("TotalMedals"))
      .orderBy(desc("TotalMedals")).limit(10)

    println("Q5")
    val top_Country_2012 = df.filter(col("Year") === "2012").groupBy("Country")
      .agg(sum("GoldMedals").alias("GoldMedals"))
      .orderBy(desc("GoldMedals")).first()

    println("Q6")
    val oldest_GoldMedalist = df.filter(col("Sport") === "Gymnastics" && col("GoldMedals").notEqual(0))
      .orderBy(desc("Age")).select("Athlete").first()

    println("Q7")
    val grp_Athlete_Sport = df.groupBy("Athlete", "Sport").count()
      .select("Athlete", "Sport", "count")

    val ath = grp_Athlete_Sport.groupBy("Athlete").count()
      .filter(col("count") > 1).select("Athlete", "count")
      .show(50)


    println("Q8")
    val teen_Athlete = df.filter(col("Age") >= 13 && col("Age") <= 19).select("Athlete")
      .distinct().count()

    println("Q9")
    val correlation = df.select(corr("Age", "TotalMedals")
      .alias("correlation"))

    println("Q10")
    val title = df.withColumn("title", when(col("GoldMedals") >= 4, "Legend")
      .when(col("GoldMedals") >= 1, "Pro").otherwise("Aspirant"))
      .groupBy(col("title")).count()

    println("Q11")
    val lewis = df.filter(col("Athlete").like("%Lewis%")).select("Athlete").distinct().show()


    println("Q12")
    val agg = df.filter(col("TotalMedals") >= 1)
      .agg(avg("Age").as("avgAge"), min("Age").as("minAge")
        , max("Age").as("maxAge")).show()

    println("Q13")

    val us_DF = df.filter(col("Country") === "United States" && col("Year")
      .isin(2000, 2002, 2004, 2006, 2008, 2010, 2012))

    var sum_Medals = us_DF.groupBy("Country", "Year")
      .agg(
        round(sum("GoldMedals")).as("GoldMedals"),
        round(sum("SilverMedals")).as("SilverMedals"),
        round(sum("BronzeMedals")).as("BronzeMedals"),
        round(sum("TotalMedals")).as("TotalMedals")
      )
      .orderBy("Year")

    println("Q14")
    var us_percentage_Medals = sum_Medals.withColumn("PercentGold", round(col("GoldMedals") / col("TotalMedals") * 100))
      .withColumn("PercentSilver", round(col("SilverMedals") / col("TotalMedals") * 100))
      .withColumn("PercentBronze", round(col("BronzeMedals") / col("TotalMedals") * 100))
      .select("Country", "Year", "PercentGold", "PercentSilver", "PercentBronze").show()

    println("Q15")
    val age_Category = df
      .withColumn("ageCat", when(col("Age").between(13, 19), "teens")
        .when(col("Age").between(20, 29), "twenties")
        .when(col("Age").between(30, 39), "thirties")
        .when(col("Age").between(40, 49), "fourties")
        .when(col("Age").between(50, 59), "fifties")
        .when(col("Age").between(60, 69), "sixties")
        .otherwise("Unknown"))

    var grp_AgeCategory = age_Category.groupBy("ageCat")
      .agg(sum("TotalMedals").alias("TotalMedals")) select("ageCat", "TotalMedals")


  }
}
