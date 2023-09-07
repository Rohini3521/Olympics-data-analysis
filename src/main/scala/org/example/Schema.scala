package org.example

import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object Schema {
  def getSchema:StructType= {
    val schema = StructType(
      Array(
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
    schema
  }
}
