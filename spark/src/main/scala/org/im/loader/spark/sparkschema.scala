package org.im
package loader
package spark

import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
 * A spark aware schema DSL.
 */
trait sparkschemacapture extends org.im.loader.schemacapture { 
  
  /** Convert this schema to a spark schema `StructType`. */
  def toSpark: StructType
  
}

