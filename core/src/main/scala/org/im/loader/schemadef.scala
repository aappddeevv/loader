package org.im
package loader

import scala.reflect.runtime.universe._
import scala.language._
import scala.util._
import cats._
import cats.data._
import java.sql._

/**
 * A simple DSL for schema definition. Simple type conversions can
 * happen here but complex ones should be placed into business rules in the mappings.
 * The tags for each attribute defined in the schema is captured so some
 * runtime (vs compile) checking can occur--better than nothing!
 *
 * A Schema is designed to assist in input record conversion from the raw input
 * record type to the type needed to run rules. It is not intended to be a
 * plain schema description DSL although it can do that. So, you define
 * the schema for the input attributes, not the targets.
 *
 * Subclasses can define other constraints, such as the maxlen on a string
 * input, because only subclasses know what the input objects look like.
 *
 * TODO: Should the converter be in the schema definition? It assumes
 * that the input objects are `Option`s.
 */
trait schemacapture {

  /** Converter takes an optional input and returns an error message or an optional value. */
  type ConverterFunction[I, R] = Option[I] => Validated[String, Option[R]]

  /** Each schema item has basic information. */
  type SchemaItem[R] >: Null <: SchemaDef[R]

  /**
   *  @param R The type that should come out of the raw input record.
   */
  trait SchemaDef[R] {
    def _name: String
    def _tag: TypeTag[R]
    def _nullable: Boolean
    def _comment: Option[String]
    def _converter: ConverterFunction[_, R]
    def _aliases: Seq[String]
    def _meta: Map[String, Any]

    def nullable: SchemaItem[R]
    def comment(c: String): SchemaItem[R]
    def required: SchemaItem[R]
    def aliases(head: String, tail: String*): SchemaItem[R]
    def meta(adds: Map[String, Any]): SchemaItem[R]
  }

  /** Add or replace a ScemaItem. */
  def addOrReplace[R](name: String, s: SchemaItem[R]): SchemaItem[R]

  /** Access the schema information by attribute name. */
  def schema: scala.collection.immutable.Map[String, SchemaItem[_]]

  /** Return the schema items in the order they were defined. */
  def schemaInOrder: Seq[(SchemaItem[_], Int)]

  /** Make a schema def with minimal information. */
  protected def mk[R](name: String, c: ConverterFunction[_, R], t: TypeTag[R]): SchemaItem[R]

  /**
   * Explicitly add a converter.
   *  @deprecated Call `add(mk(...))`
   */
  def add[R](name: String, f: ConverterFunction[_, R], tag: TypeTag[R]): SchemaItem[R] = {
    addOrReplace(name, mk(name, f, tag))
  }

  def sint(source: String)(implicit c: ConverterFunction[_, Int]) = add(source, c, implicitly[TypeTag[Int]])

  def slong(source: String)(implicit c: ConverterFunction[_, Long]) = add(source, c, implicitly[TypeTag[Long]])

  def sfloat(source: String)(implicit c: ConverterFunction[_, Float]) = add(source, c, implicitly[TypeTag[Float]])

  def sdouble(source: String)(implicit c: ConverterFunction[_, Double]) = add(source, c, implicitly[TypeTag[Double]])

  def sstring(source: String)(implicit c: ConverterFunction[_, String]) = add(source, c, implicitly[TypeTag[String]])

  def sdecimal(source: String)(implicit c: ConverterFunction[_, BigDecimal]) = add(source, c, implicitly[TypeTag[BigDecimal]])

  def sbool(source: String)(implicit c: ConverterFunction[_, Boolean]) = add(source, c, implicitly[TypeTag[Boolean]])

  /**
   *  The default parser uses a UTC converter: yyyy-mm-ddThh:mm:ssZ
   *
   *  TODO: Remove SQL date dependency.
   */
  def sdate(source: String): SchemaItem[java.sql.Date] = sdate(source, java.time.format.DateTimeFormatter.ofPattern("u-M-d'T'H:m:s'Z'"))

  def sdate(source: String, format: String) = add(source, Implicits.inputToDate(format), implicitly[TypeTag[java.sql.Date]])

  def sdate(source: String, parser: java.time.format.DateTimeFormatter) = add(source, Implicits.inputToDate(parser, true) _, implicitly[TypeTag[java.sql.Date]])

  def stimestamp(source: String)(implicit c: ConverterFunction[_, java.sql.Date]) = add(source, c, implicitly[TypeTag[java.sql.Date]])
}

