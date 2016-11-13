package org.im
package loader
package spark

import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
 * A spark aware schema DSL.
 */
trait sparkschemacapture extends org.im.loader.schemacapture { 
  
  /** Tag to convert to the right spark type. */
  private val ctypes = Seq(DataTypes.BooleanType,
      DataTypes.IntegerType,
      DataTypes.LongType,
      DataTypes.FloatType,
      DataTypes.DoubleType,
      DataTypes.StringType,
      DataTypes.DateType,      
      DataTypes.TimestampType)
  
  /** Convert this schema to a spark schema `StructType`. */
  def toSpark: StructType
  
}

