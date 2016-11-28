package org.im
package loader

import scala.reflect.runtime.universe._
import scala.language._
import scala.util.control.Exception._
import scopt._
import org.w3c.dom._
import dispatch._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.async.Async._
import scala.util._
import org.log4s._
import cats._
import cats.data._
import better.files._
import java.io.{ File => JFile }
import scala.concurrent.ExecutionContext
import java.sql._
import cats._
import cats.data._
import scala.collection.mutable.ListBuffer

/**
 *  A capability to convert entire records from an input type to an
 *  output type with a simple function. Ths is not about
 *  mappings which contain rules and business logic per attribute, but
 *  simple object transformation prior to or after running mappings.
 *
 *  The trait is more like a module. Once you instantiate the trait
 *  you can then instantiate the Converter class defined
 *  and use that object to perform the extraction/conversion.
 *  
 *  @param R The output record type. Also aliased to OutputRecord.
 */
trait RecordConverterCapability {

  /** The output record type. */
  type OutputRecord

  /**
   * The type of record input type.
   */
  type Record

  /**
   * Context input into an extract function. Extract
   *  functions are mostly dump and just transform
   *  from a simple type to the Record type.
   */
  type ConverterContext >: Null <: ConverterContextDef

  /**
   * An Extractor takes an Record and potentially returns
   * a Record.
   */
  type Converter >: Null <: ConverterDef

  /**
   * The basic input into an Extractor is the input record
   *  itself. Subclasses may add additional information or
   *  convenience functions.
   */
  trait ConverterContextDef {
    /** Raw input record type. */
    def input: Record
  }

  /**
   * Converting a record from one type to another can produce
   * errors or warnings in addition to the output values.
   */
  sealed trait ConvertResult
  /** Input record conversion resulted in an attribute specific Error. */
  case class Error(aname: String, msg: String) extends ConvertResult
  /** Input record conversion resulted in a warning. */
  case class Warning(aname: String, issue: String) extends ConvertResult

  /** A basic Extractor is a simple function. */
  trait ConverterDef extends {
    /**
     * When asked to transform a record, return either a
     *  non-empty list of errors or a `Record` result potentially with
     *  some warnings. The function should know how to handle
     *  optional/nullable/missing values and be able to
     *  encode that from ERecord to Record
     */
    def apply(e: ConverterContext): ValidatedNel[Error, (Seq[Warning], OutputRecord)]
  }

}
