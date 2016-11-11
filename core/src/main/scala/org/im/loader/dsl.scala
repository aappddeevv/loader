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
 */
trait RecordConverterCapability[R] {

  /** The output record type. */
  type OutputRecord = R

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
   * An Extractor takes an ERecord and potentially returns
   * an Record.
   */
  type Converter >: Null <: ConverterDef

  /**
   * The basic input into an Extractor is the input record
   *  itsef. Subclasses may add additional information or
   *  convenience functions.
   */
  trait ConverterContextDef {
    /** Raw input record type. */
    def input: Record
  }

  /**
   * Converting a record from one type to another
   */
  sealed trait ConvertResult
  /** Error with an attribute. */
  case class Error(aname: String, msg: String) extends ConvertResult
  /** Input record conversation resulted in a warning. */
  case class Warning(aname: String, issue: String) extends ConvertResult

  /** A basic Extractor is a simple function. */
  trait ConverterDef extends {
    /**
     * When asked to transform a record, return either a
     *  non-empty list of errors or a `Record` result with
     *  some warnings. The function should know how to handle
     *  optional/nullable/missing values and be able to
     *  encode that from ERecord to Record
     */
    def apply(e: ConverterContext): ValidatedNel[Error, (Seq[Warning], OutputRecord)]
  }

}

/**
 * Running rule for a mapping can produce 1 of 3 results.
 *  This trait feels like a Partial Function
 *  where the function may not apply at all values (hence
 *  not applicable) and that returns a value or an error,
 *  like a Partial Function that returns a Try or Either.
 *  We do not cast this as a Partial Function because
 *  the calculation to see if the function applies could
 *  be quite expensive and needs to be done inline with
 *  the actual calculation.
 *
 *  TODO: Make this a monad although that would fly
 *  in the face of the Rules based model. Think about whether
 *  we would want to do that.
 */
sealed trait RuleResult
/** Rule produced a valid output value, which can be None. */
case class Value[R](value: Option[R]) extends RuleResult
/** Rule ran into an error and it should be communicated somehow. */
case class Error(msg: String) extends RuleResult
/** Rule was not applicabe to the input record. */
case class NotApplicable(reason: Option[String] = None) extends RuleResult

/**
 * Top level trait for data mappings. Data mappings
 * are a collection of mapping definitions. Each
 * mapping definition has a set of rules that can
 * be applied to an input record. There are three stages to
 * mappings, (e)xtract from the input Recrod, (t)ransform
 * from the input record to an output format and (l)oad where
 * the transformed data is packed into some type of output.
 * The ETL concept is inherent in a single mappings object as well
 * as across mappings objects that are composed together.
 *
 * @param Record Input record type.
 */
trait datamappings {

  /**
   * Input type for running mappings. A mappings implementation
   *  may choose to present a different representation of the data
   *  to the rules but that "internal" representation would usually be
   *  bundled under the RuleContext. The mapping itself (and rules)
   *  runs after the data has been extracted from the ERecord.
   */
  type Record

  /** Rule context is input into a rule. */
  type RuleContext >: Null <: RuleContextDef

  /** Rules are collected together in a mapping definition. */
  type Rule[R] >: Null <: RuleDef[R]

  /**
   * A specific mapping that contains rules.
   *
   *  @param I Input type of the mapping.
   *  @param R The output type of the mapping.
   */
  type Mapping[R] >: Null <: MappingDef[R]

  /**
   * Main input into a rule when it is being run.
   *
   *  We need to genericize `isInRecord` and `isAValue`
   *  so that it does not depend on strings for column
   *  names.
   */
  trait RuleContextDef extends Dynamic {
    /** The input record. */
    def input: Record

    /** If the input record contains an attribute. */
    def isInRecord(name: String): Boolean

    /**
     * If the input record contains an attribute called name
     *  but the attribute does not contain a value e.g.
     *  it's null or None or some other marker indicating
     *  the lack of a value for the input record.
     */
    def isAValue(name: String): Boolean
  }

  /**
   * A rule has a priority and some way to execute a function that takes
   * a context and retuns an optional value or an error message. A
   * priority controls the order of the rules firing so essentially
   * they sequence the rules, conceptually the same as flatMap,
   * except you can stop the evaluation of the rules on the first
   * successful result--conceptually the same as running a Free monad (:-).
   *
   * @param C Input Context
   * @param F Effect return values is wrapped in.
   * @param R Type of result value
   * @param E Executor that executes the rule.
   */
  trait RuleDef[R] {

    /** Convenience type alias. */
    type RuleExec = RuleContext => RuleResult

    /** Priority of rule. */
    def priority: Int

    /** Function that runs the rule. */
    def f: RuleExec

    /** RuleDef is really just a function. */
    def apply(ctx: RuleContext): RuleResult = f(ctx)

    /** Optional label for the rule. */
    def label: Option[String]
  }

  /**
   * An Extractor that can extract from raw input
   *  into an output that is suitable as the input
   *  into the mappings stage.
   */
  type Extractor >: Null <: RecordConverterCapabilityDef

  /** An extraction capability that outputs a record of type `Record`. */
  trait RecordConverterCapabilityDef extends RecordConverterCapability[Record]

  /**
   * The extractor that acts as a pre-processor for this mappings object.
   *  The extractor contains a class that can be instantiated and used
   *  to convert input records into output records needed by the
   *  mappings. You should think of the `extractor`as a module with class
   *  definitions inside used to create objects to help you with the extraction.
   */
  def extractor: Extractor

  /**
   *
   * A mapping definition is really a thin wrapper around a set of
   * rules. The "T" and "L" in "ETL" are broken into two
   * different models. The "T" is represented by business rules
   * which create a jvm type as output.
   *
   * @param R The output type of the mapping.
   */
  trait MappingDef[R] {

    type This >: Null <: MappingDef[R]

    /** A mapping definition has an internally managed id. */
    def _id: Int
    /** If the mapping started out source biased, this was the source specified. */
    def source: Option[String]
    /** For a given target, it my consume multiple sources. This is a documentation list. */
    def sources: Seq[String]
    /** The target output attribute. */
    def target: String
    /** Description. */
    def description: Option[String]
    /** Mapping should be skipped, not run. */
    def skip: Boolean
    /** The output can be null/not a value/none */
    def nullable: Boolean
    /** A not on the type of this mapping. */
    def typeNote: Option[String]
    /** Mapping rules. */
    def trules: Seq[Rule[R]]

    // Operations that require the proper type return, could move to a typeclass ops thing

    def changeSource(s: Option[String]): This
    def changeTarget(t: String): This
    def changeDescription(d: Option[String]): This
    def changeSkip(s: Boolean): This
    def changeNullable(n: Boolean): This
    def changeTypeNote(n: Option[String]): This
    def changeTRules(r: Seq[Rule[R]]): This
    def mkRule(priority: Int, f: RuleContext => RuleResult, note: Option[String] = None): Rule[R]
    def updateMapping(m: This): This
    def changeSources(n: Seq[String]): This

    override def hashCode: Int = _id

    override def equals(that: Any): Boolean =
      that match {
        case that: MappingDef[_] => this._id == that._id
        case _ => false
      }

    protected lazy val logger = getLogger("mappingdef")
    override def toString = s"MappingDef[id=${_id}, source=$source, target=$target, description=$description, skip=$skip, sources=${source.mkString(",")}, ...]"

    /** Sets the sources attributes. */
    def sources(n: String, tail: String*): This = sources(Seq(n) ++ tail)

    /** Sets the sources atribute. */
    def sources(slist: Seq[String]): This = updateMapping(changeSources(slist))

    /**
     * Change the source attribute. This forces the transform to only
     *  operate on one input if the rule expects it e.g. a direct move.
     */
    def source(newSource: String) = updateMapping(changeSource(Option(newSource)))

    /** Whether this mapping should be used. */
    def shouldMap: Boolean = !this.skip

    /**
     * Changes the target.
     *  @deprecated
     */
    def to(col: String) = updateMapping(changeTarget(col))

    /** Provide a description for the mapping. */
    def desc(t: String) = updateMapping(changeDescription(Some(t)))

    def addToDesc(d: String) = {
      val dd = this.description.map(_ + " " + d) orElse Option(d)
      updateMapping(changeDescription(dd))
    }

    /** Always map to a None value so that the database gets a null. */
    def mapToNoValue = constant(None).addToDesc("Mapped to 'no value.'")

    /**
     * If the input has aname in the input record but its None, use the default
     *  value `c`. Return an error if the input record does not have aname in
     *  the input record. If aname is in the input record and it has a value,
     *  return NotApplicable so other rules can run. This combinator is semantically
     *  equivalent to setting a default value.
     */
    def ifNotPresent(aname: String, c: Option[R]) =
      rule(-1000) { ctx =>
        if (!ctx.isInRecord(aname)) Error(s"Input record does not contain attribute $aname for target $target.")
        else if (!ctx.isAValue(aname)) Value(c)
        else NotApplicable()
      }

    /**
     * Create a rule that always returns a constant value.
     * Once this rule is run, no other rules following it will run.
     */
    def constant(c: Option[R]) =
      rule(-1000) { ctx =>
        Value(c)
      }.addToDesc(s"Set to a constant value $c.")

    //    /** Set target name to same as source name, if source is defined. */
    //    def toSameCol: This = {
    //      source.foreach(s => updateMapping(changeTarget(s)))
    //      this.asInstanceOf[This]
    //    }

    /** Zap all existing rules that are defined on the mapping. */
    def clearRules: This = updateMapping(changeTRules(Seq()))

    /** Do not map. Just a placeholder mapping. */
    def ignore = updateMapping(changeSkip(true))

    /** Create a rule for a target. Add to any existing list of rules. */
    def rule(priority: Int, label: Option[String] = None)(f: RuleContext => RuleResult): This = {
      if (trules.map(_.priority).contains(priority)) sys.error(s"Rule for target $target with priority $priority already exists.")
      val tmp = (this.trules :+ mkRule(priority, f, None))
      updateMapping(changeTRules(tmp))
    }

    /**
     * Define a default rule with priority 1 greater than the last priority
     *  in the list of current rules.
     */
    def rule(f: RuleContext => RuleResult): This = {
      val nextPriority = if (trules.size == 0) 0 else (trules.map(_.priority).last + 1)
      rule(nextPriority)(f)
    }

    /** Do not allow nulls. */
    def required = updateMapping(changeNullable(false))

    /** Allow "missing values" propagate. */
    def notRequired = updateMapping(changeNullable(true))

    /** Add a note about the type. Adds to the note rather than replace. */
    def typenote(note: String) = {
      val newnote = this.typeNote.map(_ + " " + note) orElse Option(note)
      updateMapping(changeTypeNote(newnote))
    }

    /** Replace current typenote and add this one. */
    def replaceTypenote(note: String) = updateMapping(changeTypeNote(Some(note)))
  }
}

object MappingDef {
  val atomic = new java.util.concurrent.atomic.AtomicInteger
  def generateId: Int = atomic.incrementAndGet
}


/**
 * Implicits for type conversion. Many of them return a Validated
 * object from the cats library instead of a RuleResult because they
 * can be used in different contexts other than rules.
 */
object Implicits {
  import cats.implicits._
  import Validated._

  def toMapWithOptionValues(input: Map[String, String]): Map[String, Option[String]] =
    input.map { case (k, v) => (k, if (v.isEmpty) None else Option(v)) }.toMap

  /**
   * Sequence values from values in the order of keys.
   *  Throw an exception if values does not contain any of the keys.
   *
   *  This is a helper function for clients writing loaders.
   */
  def sequenceLikeUnsafe[T](keys: Seq[String], values: Map[String, T]) =
    keys.map(k => values(k))

  /**
   * Convert a validated to a rule result. Left is an Error
   *  and Right is a Value. If you have something that returns
   *  a Validated[String, Option[R]] and you want a RuleResult,
   *  you can do a `yourmethod _ andThen toRuleResult` (or
   *  drop the `_` if its a function object).
   *
   *  @param v The Validated result.
   */
  def toRuleResult[R](v: Validated[String, Option[R]]): RuleResult =
    v.fold(msg => Error(msg), x => Value[R](x))

  /**
   * A more flexible toResultResult where you choose what the left side
   *  becomes. Double parameter list makes it easy to curry.
   *  @param leftIsError Left side becomes an Error if true, otherwise NotApplicable.
   */
  def toRuleResultC[R](leftIsError: Boolean)(v: Validated[String, Option[R]]): RuleResult =
    v.fold(msg => if (leftIsError) Error(msg) else NotApplicable(Option(msg)), x => Value[R](x))

  /** Convert a boolean string such as true/t/1 or false/f/0 to a small integer 1 or 0 respectively. */
  def booleanToInt(v: Option[String]): Validated[String, Option[Int]] = v.filterNot(_.isEmpty).fold(valid[String, Option[Int]](None)) { b =>
    val bu = b.toUpperCase
    if (bu == "TRUE" || bu == "T" || bu == "1") Validated.valid(Option(1))
    else if (bu == "FALSE" || bu == "F" || bu == "0") Validated.valid(Option(0))
    else Validated.invalid(s"Unable to convert $b to a 0 or 1.")
  }

  /** Convert a string in UTC format to a sql date. Can't use ISO_DATE directly. */
  def utcConvert = inputToDate(java.time.format.DateTimeFormatter.ofPattern("u-M-d'T'H:m:s'Z'")) _
  //Validated.catchOnly[java.time.format.DateTimeParseException] { v.filterNot(_.isEmpty).map(d => new java.sql.Date(java.time.Instant.parse(d).toEpochMilli())) }.leftMap(_.getMessage)

  import java.time._
  import java.time.format._

  /** Data converter using a string format. */
  def inputToDate(format: String): Option[String] => Validated[String, Option[java.sql.Date]] =
    inputToDate(java.time.format.DateTimeFormatter.ofPattern(format)) _

  /**
   * General purpose date converter.
   *
   * @param formatter Parser.
   * @param invalidIfParseError Convert an Invalid to a Valid(None) if false. Default is true, parse errors are Invalid.
   */
  def inputToDate(formatter: DateTimeFormatter, invalidIfParseError: Boolean = true)(iopt: Option[String]): Validated[String, Option[java.sql.Date]] = {
    val tmp = Validated.catchOnly[java.time.format.DateTimeParseException](iopt.map { strvalue =>
      val ld = java.time.LocalDate.parse(strvalue, formatter)
      java.sql.Date.valueOf(ld)
    }).leftMap(_.getMessage)

    if (tmp.isInvalid && !invalidIfParseError) None.valid
    else tmp
  }

  /**
   * Converts to a type and catches exceptions. Empty strings are automatically
   *  mapped to a None but "valid" value. An Exception's message becomes the
   *  error message. You can use `toRuleResult` to return a RuleResult.
   */
  def stringTo[R](f: String => R)(v: Option[String]) = Validated.catchOnly[NumberFormatException] { v.filterNot(_.isEmpty).map(f(_)) }.leftMap(_.getMessage)

  implicit val asLong = stringTo[Long](_.toLong) _
  implicit val asInt = stringTo[Int](_.toInt) _
  implicit val asString = stringTo[String](x => x) _
  implicit val asBigDecimal = stringTo[BigDecimal](v => BigDecimal(v)) _
  implicit val asFloat = stringTo[Float](_.toFloat) _
  implicit val asDouble = stringTo[Double](_.toDouble) _
  implicit val asDate = utcConvert _
  implicit val asTimestamp = (v: Option[String]) =>
    Validated.catchOnly[IllegalArgumentException] { v.filterNot(_.isEmpty).map(s => java.sql.Timestamp.valueOf(s)) } leftMap { _.getMessage }

  /**
   * Hack job...a library should have this somewhere that is much smarter.
   *  This only fixes type signatures but does not translate different
   *  representations.
   */
  implicit class EnrichedMap(m: Map[String, Option[_]]) {
    /** Get a typed optional value out of the map. You could get a Some(r) or None. */
    def getAs[R](k: String): Option[R] = m.get(k).flatMap(_.map(_.asInstanceOf[R]))

    /** Get a value directly but throw an exception if the value does not exist. You can get a R or an exception. */
    def as[R](k: String): R = {
      val tmp = m.get(k)
      tmp.flatMap(_.map(_.asInstanceOf[R])).getOrElse(throw new RuntimeException(s"Could not convert value $tmp to proper type."))
    }
  }

}
