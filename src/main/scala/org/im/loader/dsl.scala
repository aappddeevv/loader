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
import fs2._
import scala.concurrent.ExecutionContext
import java.sql._
import cats._
import cats.data._
import scala.collection.mutable.ListBuffer
import com.lucidchart.open.relate.interp._

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
   * @parma E Executor that executes the rule.
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
 * A set of mappings specified by a self-documenting DSL. Mappings can be
 * created that are source-side centric or target-side centric. Mappings
 * are created as a set of rules to be applied to an input record.
 *
 * This class assumes records are maps of column names to strings.
 *
 * You will need import `interp` otherwise you have to explicitly
 * provide a function to take your transform output and create
 * a `Parameter`:
 * {{{
 * import com.lucidchart.open.relate.interp._
 * }}}
 */
abstract case class mappings(entity: String, table: String, schema: Option[String] = None)
    extends datamappings { self =>
  import java.time._
  import java.time.format._
  import java.time.temporal._
  import cats._
  import cats.data._
  import cats.instances.all._
  import cats.implicits._

  private[this] lazy val logger = getLogger

  type Record = Map[String, Option[_]]
  type Mapping[R] = BasicMapping[R]
  type Rule[R] = BasicRule[R]
  type RuleContext = BasicRuleContext
  type Extractor = MapExtractionCapability

  /** Input schema definition to extract from the input record type. */
  class schema extends schemacapture {
    type SchemaItem[R] = SchemaDef[R]
    case class SchemaDef[R](
        _name: String,
        _tag: TypeTag[R],
        _nullable: Boolean = true,
        _comment: Option[String] = None,
        _converter: ConverterFunction[_, R],
        _aliases: Seq[String] = Seq()) extends super.SchemaDef[R] {
      def nullable: SchemaItem[R] = copy(_nullable = true)
      def required: SchemaItem[R] = copy(_nullable = false)
      def comment(c: String): SchemaItem[R] = copy(_comment = Option(c))
      def aliases(head: String, tail: String*): SchemaItem[R] = copy(_aliases = Seq(head) ++ tail)
    }

    private val _schema = scala.collection.mutable.HashMap.empty[String, SchemaItem[_]]
    def schema = _schema.toMap
    def addOrReplace[R](name: String, s: SchemaItem[R]): SchemaItem[R] = { _schema(name) = s; s }
    protected def mk[R](name: String, c: ConverterFunction[_, R], t: TypeTag[R]): SchemaItem[R] = SchemaDef(name, t, _converter = c)
    // Bring converters appropriate to the `Record` input type.
    import Implicits._

    private def truncate(input: Option[String]) = input.map(s => org.apache.commons.lang3.StringUtils.substring(s, 0, 99)).valid[String]

    def sstring(source: String, maxLen: Int) = add(source, truncate, implicitly[TypeTag[String]])
  }

  /**
   * Input schema. Override this in your subclass to define your input schema.
   * Simple conversions can be performed from the input record formats
   * to the input record used in rules. Complex conversions should be handled
   * in rules.
   *  {{{
   *  override object ischema extends schema {
   *     slong("colasource").comment("Col A")
   *     sdate("colbsoruce").required // not nullable
   *  }
   *  }}}
   */
  val ischema = new schema()

  /** Convert from CSV reader based on strings to some specific types. */
  class MapExtractionCapability extends super.RecordConverterCapabilityDef {

    type Record = Map[String, Option[String]]
    type ConverterContext = RecordAndIdx
    type Converter = StringMapExtractor

    type AttributeConverter = Option[String] => Validated[String, Option[_]]
    type SourceGetter = () => Option[String]

    private val converters = scala.collection.mutable.HashMap[SourceGetter, AttributeConverter]()

    /** Add a converter, can't distinguish between adding the same mappingdef. */
    def +=(p: (Mapping[_], AttributeConverter)) = {
      converters += (() => if (!p._1.skip && p._1.source.isDefined) p._1.source else None, p._2)
    }

    def add(attr: String, f: AttributeConverter) = {
      converters += (() => Option(attr), f)
    }

    private def getConverters = converters ++ ischema.schema.map { case (k, v) => (() => Option(k), v._converter.asInstanceOf[AttributeConverter]) }

    case class RecordAndIdx(input: Record, idx: Int) extends super.ConverterContextDef

    case class StringMapExtractor() extends super.ConverterDef {

      def apply(e: ConverterContext): ValidatedNel[Error, (Seq[Warning], OutputRecord)] = {
        val converted = getConverters.map {
          case (m, f) if (m().isDefined) =>
            val source = m().get
            // input record must have the source value available, even if its null
            val tmp = e.input.get(source).map(v => f(v))
            (source, tmp)
        }
        val errors = converted.collect { case (name, Some(Validated.Invalid(e))) => (name, Error(name, e)) } ++
          converted.collect { case (name, None) => (name, Error(name, s"Attribute $name not found in input record.")) }
        val valid = converted.collect { case (name, Some(Validated.Valid(output))) => (name, output) }

        //println(s"extraction produced: converted=$converted\nerrors=$errors\nvalid=$valid")

        if (errors.size > 0)
          NonEmptyList.fromListUnsafe(errors.values.toList).invalid
        else
          (Seq.empty[Warning], e.input ++ valid).validNel
      }
    }
  }

  val extractor = new MapExtractionCapability()

  class ParameterOutputCapability extends RecordConverterCapability[Map[String, Parameter]] {
    type Record = Map[String, Option[_]]
    type ConeverterContext = RecordAndIdx
    type Converter = StringToOptionConverter

    case class RecordAndIdx(input: Record, idx: Int) extends super.ConverterContextDef

    case class StringToOptionConverter(converters: Map[String, Option[_] => Parameter]) extends super.ConverterDef {
      def apply(e: ConverterContext): ValidatedNel[Error, (Seq[Warning], OutputRecord)] = {
        val converted = converters.map { case (name, f) => (name, e.input.get(name).map(v => f(v))) }
        val errorAttributes = converted.filter { case (k, v) => v.isEmpty }.keySet.toList
        if (errorAttributes.size > 0)
          NonEmptyList.fromListUnsafe(errorAttributes.map(a => Error(a, "Unable to obtain Parameter object."))).invalid
        else
          (Seq.empty[Warning], converted.mapValues(_.get)).validNel
      }
    }
  }

  val loader = new ParameterOutputCapability()

  /**
   * Context for rule firing input. Also contains convenience methods for
   *  return values from the rule firing.
   */
  case class BasicRuleContext(val input: Record) extends super.RuleContextDef {
    /** Call this to return a successful value from a rule. */
    def success[R](v: Option[R]) = Value(v)

    /** Return a raw value as successful. */
    def success[R](v: R) = Value(Option(v))

    /** Return a None value as a successful firing. None usually translates to a target system "null" concept. */
    def none[R] = Value(None)

    /** Call this to return an unsuccessful value from a rule. */
    def notApplicable[R](msg: String) = NotApplicable()

    /** Return an unsuccessful rule firing result. */
    def notApplicable[R] = NotApplicable(Option("Rule does not apply."))

    /** Return an error. */
    def error(msg: String) = Error(msg)

    def isInRecord(name: String): Boolean = input.contains(name)

    def isAValue(name: String): Boolean = input.get(name).map(_.isDefined).getOrElse(false)

    /**
     * Get a value dynamically. Dangereous to use. Return value will be an Option[_]
     * or throw a RuntimeException.
     *  Use this like:
     *  {{{
     *  ctx.colasource[String] + "-suffix"
     *  ...
     *  ctx.colbsource[Long] + 100
     *  }}}
     *
     */
    def selectDynamic[R](methodName: String): Option[R] = {
      if (!input.contains(methodName)) throw new RuntimeException(s"Attribute $methodName does not exist in input record.")
      input.get(methodName).get.asInstanceOf[Option[R]]
    }
  }

  case class BasicRule[R](val priority: Int, val f: RuleContext => RuleResult,
    val label: Option[String] = None) extends super.RuleDef[R]

  case class BasicMapping[R](
      _id: Int = MappingDef.generateId,
      source: Option[String] = None,
      sources: Seq[String] = Seq(),
      target: String,
      description: Option[String] = None,
      skip: Boolean = false,
      nullable: Boolean = true,
      typeNote: Option[String] = None,
      trules: Seq[Rule[R]] = Seq(),
      loader: Option[Option[R] => Parameter] = None) extends super.MappingDef[R] {

    type This = BasicMapping[R]

    // Methods that require returning a specifically typed record.

    def changeSource(s: Option[String]) = copy(source = s)
    def changeTarget(t: String) = copy(target = t)
    def changeDescription(d: Option[String]) = copy(description = d)
    def changeSkip(s: Boolean) = copy(skip = s)
    def changeNullable(n: Boolean) = copy(nullable = n)
    def changeTypeNote(n: Option[String]) = copy(typeNote = n)
    def changeTRules(r: Seq[Rule[R]]) = copy(trules = r)
    def changeSources(s: Seq[String]) = copy(sources = s)
    def mkRule(priority: Int, f: RuleContext => RuleResult, label: Option[String] = None) = BasicRule[R](priority, f, label)
    def updateMapping(m: This): This = self.updateMapping(m)

    /** Combinator to update loader. */
    def loader(f: Option[R] => Parameter): This = copy(loader = Option(f))

    /**
     * Just move the value over directly assuming the source name
     * is the same as the target name. This rule never returns
     * NotApplicabe so no other
     *  rules following this one will run. The type of the value in the input
     *  record must match the mapping's output type--you can add the source
     *  as an entry to the ischema.
     *  {{{
     *    to[Long]("colf").directMove
     *  }}}
     *  The default priority is last so you can have other rules in between
     *  and eventually add this as the last and final rule.
     *
     *  If the source has not been specified already, it is set to the target attribute
     *  name.
     */
    def directMove(priority: Int): This = {
      rule(priority) { ctx =>
        import Implicits._
        val source = target
        if (!ctx.isAValue(source) && nullable) Value(None)
        else Value(ctx.input.getAs[R](source))
      }.source(target).sources(target)
    }

    /** Direct move rule that always runs after all other rules have run. */
    def directMove: This = directMove(Int.MaxValue)

    /**
     * Create a rule that sources a value from `source`. If the input value
     * is not the right type, return NotApplicable, otherwise return the
     * converted value. This rule allows other rules to be
     * added unlike `directMove`. If any other exception is thrown, return Error.
     *
     * Usage:
     * {{{
     *   to[Int]("targetcol").from[Int]("sourcecol") // if same type
     *   ...
     *   to[Int]("targetcol2").from[Float]("sourcecol2", _.toInt)
     * }}}
     * assuming you had a schema entry to turn sourcecol into an Int. You can
     * chain multiple `fromX` with different input type `S`.
     *
     * @param source The source attribute.
     * @param f A function that takes a `S` to an `R`. Default is straight conversion.
     * f is only used if there is a non-None value in the input that needs type conversion.
     * @param S The source in the input record. If you have asked for a conversion
     * in the schema, you need to match up the type that you specified in the schema to S.
     */
    def fromX[S](source: String, f: S => R = (v: S) => v.asInstanceOf[R], priority: Int = Int.MaxValue) =
      rule(priority) { ctx =>
        import Implicits._
        try {
          toRuleResultC(false)(Validated.valid(ctx.input.getAs[S](source).map(f)))
        } catch {
          castclassisna(source) andThen catchblock(source)
        }
      }.sources(sources ++ Seq(source))

    /**
     * @deprecated
     */
    def from(source: String)(implicit converter: Option[String] => Validated[String, Option[R]]) = {
      this.rule { ctx =>
        import Implicits._
        toRuleResultC(false)(converter(ctx.input.getAs[String](source)))
      }
    }.sources(sources ++ Seq(source))

    /**
     * Rule that extracts a single value from the input record runs a function. Return
     * an Error if the input value does not exist. Exceptions are mapped to Errors.
     *
     * @param priority The rule's priority.
     * @param f Function that takes a context and input value and outputs a rule result.
     * @param S The value's type in the input record.
     *
     * TODO: Match typetag from schema, if defined, with type expected in the single value.
     */
    def rule1[S](priority: Int)(source: String)(f: (RuleContext, Option[S]) => RuleResult)(implicit tag: TypeTag[S]): This = rule(priority) { ctx =>
      val inputValue = ctx.input.get(source)
      try {
        inputValue.map { v =>
          f(ctx, v.asInstanceOf[Option[S]])
        } getOrElse Error(s"No source attribute $source exists in the input record for target $target")
      } catch { catchblock(source) }
    }.sources(sources ++ Seq(source))

    def castclassisna(source: String): PartialFunction[Any, RuleResult] = {
      case x: java.lang.ClassCastException => NotApplicable(Option(s"Wrong type for value $source -> $target."))
    }

    def catchblock(source: String): PartialFunction[Any, RuleResult] = {
      case x: java.lang.ClassCastException =>
        Error(s"Could not change input type for attribute input $source and target $target.")
      case scala.util.control.NonFatal(e) =>
        Error(s"Error executing rule with single attribute input $source and target $target: ${e.getMessage}")
    }
  }

  /**
   * A builder type that allows starting a mapping definition using
   *  the source side of the mapping versus target centric.
   *
   *  TODO: Create a small tracker within the mappings class that
   *  can detect when an incomplete mapping has not been completed.
   *  This would run at runtime and help detect mapping errors.
   */
  sealed trait IncompleteMapping[R] {
    def source: String
    def callback: Mapping[R] => Mapping[R]
  }

  /**
   * Used when you start with a source-side mapping. A source side mapping
   *  needs to have some `to` information added to be completed and recorded
   *  as a valid mapping. When you start with the source side, you have
   *  a very limited vocabulary.
   */
  case class NoTargetDefined[R](source: String, callback: Mapping[R] => Mapping[R] = (m: Mapping[R]) => m) extends IncompleteMapping[R] {

    /** Sets up direct move with a given priority.. */
    def directMove(priority: Int)(implicit loader: Option[R] => Parameter) =
      callback(self.to[R](source).fromX[R](source, priority = priority).sources(source).source(source)).addToDesc(s"Direct move.")

    /** Sets up a direct move including the transfer logic. */
    def directMove(implicit loader: Option[R] => Parameter) =
      callback(self.to[R](source).fromX[R](source).sources(source).source(source)).addToDesc(s"Direct move.")

    /** Sets up a move to target including the transfer logic. Use `to_` if you need to just set the target then add your own rules.. */
    def to(target: String, priority: Int = Int.MaxValue)(implicit loader: Option[R] => Parameter) =
      callback(self.to[R](target).fromX[R](source, priority = priority).sources(source).source(source)).addToDesc(s"Direct move.")

    /** Create the mapping without any logic and marks it to be ignored. */
    def ignore(implicit loader: Option[R] => Parameter) =
      callback(self.to[R](source).sources(source).source(source).ignore)

    /**
     * Creates the mapping with target and sets the source but does not add any rules or transfer logic.
     *  Use this `to` if you are going to add your own rules after.
     */
    def to_(target: String, priority: Int = Int.MaxValue)(implicit loader: Option[R] => Parameter) =
      callback(self.to[R](target).sources(source).source(source)).addToDesc(s"Direct move.")

    /**
     * Sets the target to the source but does not add any transfer/rule logic.
     *  Use this function when you want to do a direct move but add your
     *  logic in business rules.
     */
    def directMove_(implicit loader: Option[R] => Parameter) = to_(source)
  }

  protected val _mappings = new ListBuffer[Mapping[_]]

  /**
   * Call before using the mappings to confirm their validity.
   *  You'll need to override the definition in your subclass.
   */
  def isValid: ValidatedNel[String, Boolean] = {
    val t = mappingsWithDuplicativePriorities
    if (t.size > 0) {
      val msgs = t.map(e => e._2 + ": " + e._3)
      Validated.invalid(NonEmptyList.fromListUnsafe(msgs.toList))
    } else
      true.validNel[String]
  }

  /** Report an error out. */
  def reportError(msg: String): Unit = {
    Console.err.println("Error: " + msg)
  }

  /** Report a warning to the console. */
  def reportWarning(msg: String): Unit = {
    Console.err.println("Warning: " + msg)
  }

  /** Generate an internal processing status report. */
  def report(): String = ""

  /**
   * Start a mapping. Straight forward source-to-target with schema conversion to `I` and output `R`. Target name is the same as the source name.
   *
   *  @deprecated Use `to`
   */
  def mapping[I, R](source: String)(implicit extractor: Option[String] => Validated[String, Option[I]],
    s: Option[R] => Parameter) = { val d = makeDef[R](target = source).source(source).loader(s); self.extractor += (d, extractor); d }

  /**
   * A mapping of some sort. Straight forward extractor `I` and output `R` source-to-target.
   *
   *  @deprecated Use `to`
   */
  def mapping[I, R](source: String, target: String)(implicit extractor: Option[String] => Validated[String, Option[I]],
    s: Option[R] => Parameter) = { val d = makeDef[R](target = target).source(source).loader(s); self.extractor += (d, extractor); d }

  /**
   * A mapping with the source and target defined as the same name. You need to add rules
   * in order for the mapping to fire. Using this function automatically adds
   * an extractor into the input schema.
   *
   * @param I Input schema type specification.
   * @param R Mapping output type.
   */
  def to[I, R](target: String)(implicit extractor: Option[String] => Validated[String, Option[I]],
    loader: Option[R] => Parameter) = { val d = makeDef[R](target).loader(loader).source(target); self.extractor += (d, extractor); d }

  /**
   * Creates a target with no rules. No extractor is setup so you need to
   *  ensure the type is converted through a rule or use the input as is.
   *  No rules are setup on this target unless you add some.
   *
   *  @param R Mapping output type.
   */
  def to[R](target: String)(implicit loader: Option[R] => Parameter) = makeDef[R](target).loader(loader)

  /**
   * Add a rule to an existing target or create a new one. This is preferred
   * over `to` when defining rules in separate mapping statements.
   *
   *  You can get R matching errors this way! Need to use Classtag somehow!?!?!?
   */
  def ruleFor[I, R](target: String)(priority: Int)(f: RuleContext => RuleResult)(implicit extractor: Option[String] => Validated[String, Option[I]], s: Option[R] => Parameter) = {
    val existing = allMappings.find(_.target == target)
    existing.fold {
      val tmp = makeDef[R](target).loader(s)
      self.extractor += (tmp, extractor)
      tmp.rule(priority, None)(f)
    }(existing => existing.asInstanceOf[Mapping[R]].rule(priority)(f)) // val tmp above makes BasicMapping local to tmp path dependent type, so recast
  }

  /** Target columns. Sort the names the way you want to create target sql commands. */
  def targetCols: Seq[String] = mappings.map(_.target)

  import com.lucidchart.open.relate.interp.Parameter._

  /**
   * Combinators that allow you to start specifying mappings source first.
   *  It is more difficult to specify mappings source first compared to
   *  target first. When you are using "sourcefirst", you specify the schema type and the source
   *  field then use combinators to finish the mapping:
   *  {{{
   *   long("longattribute").to("targetforlongattribute")
   *   }}}
   *   A schema entry is automatically created for `longattribute`.
   *   If you were to specfy just `long("longattribute")` you would
   *   have an incomplete mapping.
   */
  trait sourcefirstdef {
    /**
     * Converts from UTC: 2016-01-01T01:01:01Z. Source biased.
     */
    def date(name: String, errorIfParseError: Boolean = false) = dateWithFormatter(name, DateTimeFormatter.ofPattern("u-M-d'T'H:m:s'Z'"), errorIfParseError)

    /** Parse according to date format. */
    def dateWithFormat(name: String, format: String, errorIfParseError: Boolean = false) =
      dateWithFormatter(name, DateTimeFormatter.ofPattern(format), errorIfParseError, Option(s"Custom date formatter: $format"))

    /**
     * Parse according to DateTimeFormatter. Source biased.
     *
     *  @param name Source attribute
     *  @param formatter Date parser instance.
     *  @param errorIfParseError If the input string does not parse, return an Error if true, NotApplicable otherwise.
     */
    def dateWithFormatter(name: String, formatter: DateTimeFormatter, errorIfParseError: Boolean = false, desc: Option[String] = None) =
      NoTargetDefined[java.sql.Date](name, (m: Mapping[java.sql.Date]) => {
        import Implicits._
        m.rule { ctx =>
          val f = inputToDate(formatter, errorIfParseError) _ andThen toRuleResultC(errorIfParseError)
          f(ctx.input.getAs[String](name))
        }.typenote("Date formatter.").addToDesc(desc.getOrElse(""))
      })

    /** Source biased. */
    def string(name: String) = {
      ischema.add(name, Implicits.asString, implicitly[TypeTag[String]])
      NoTargetDefined[String](name)
    }

    /** Source biased. */
    def strbool(name: String) = {
      ischema.add(name, Implicits.booleanToInt _, implicitly[TypeTag[Int]])
      NoTargetDefined[Int](name, callback = (m: Mapping[Int]) => m.typenote("Boolean string to int."))
    }

    /** Source biased. */
    def duration(name: String) = long(name)

    /** Source biased. */
    def int(name: String) = {
      ischema.add(name, Implicits.asInt, implicitly[TypeTag[Int]])
      NoTargetDefined[Int](name)
    }

    /** Source biased. */
    def long(name: String) = {
      ischema.add(name, Implicits.asLong, implicitly[TypeTag[Long]])
      NoTargetDefined[Long](name)
    }

    /** Source biased. */
    def float(name: String) = {
      ischema.add(name, Implicits.asFloat, implicitly[TypeTag[Float]])
      NoTargetDefined[Float](name)
    }

    /** Source biased. */
    def double(name: String) = {
      ischema.add(name, Implicits.asDouble, implicitly[TypeTag[Double]])
      NoTargetDefined[Double](name)
    }

    /** Source biased. */
    def decimal(name: String) = {
      ischema.add(name, Implicits.asBigDecimal, implicitly[TypeTag[BigDecimal]])
      NoTargetDefined[Int](name)
    }

    /** Source biased. */
    def timestamp(name: String) = {
      ischema.add(name, Implicits.asTimestamp, implicitly[TypeTag[java.sql.Timestamp]])
      NoTargetDefined[java.sql.Timestamp](name)
    }
  }

  /** Import source "first" mapping combinators. */
  val sourcefirst = new sourcefirstdef {}

  /**
   *  Get all mappings that should be mapped. Mappings that are not
   *  skipped and have rules defined.
   */
  def mappings = mappingsWithRules

  /** Get all mappings. */
  def allMappings = _mappings

  /**
   * Convenience function. *Not* case sensitive.
   *  Finds a mapping for a given target.
   */
  def get(name: String) = {
    val ncaps = name.toUpperCase
    mappings.find(_.target.toUpperCase == ncaps)
  }

  private[loader] def makeDef[R](target: String): Mapping[R] =
    updateMapping(new BasicMapping[R](target = target))

  /**
   * Update a mapping. If the mapping already exists, by matching on the _id,
   *  then just update the mapping in the list.
   */
  private[loader] def updateMapping[R](mapping: Mapping[R]): Mapping[R] = {
    val idx = _mappings indexWhere { _._id == mapping._id }
    if (idx > -1) _mappings(idx) = mapping
    else _mappings += mapping
    mapping
  }

  //  /** Produce a (potentially infinite) stream from an unfold. */
  //  def unfold[F[_],S,A](s0: S)(f: S => Option[(A,S)]): Stream[F,A] = {
  //    def go(s: S): Stream[F,A] =
  //      f(s) match {
  //        case Some((a, sn)) => emit(a) ++ go(sn)
  //        case None => empty
  //      }
  //    suspend(go(s0))
  //  }

  /**
   * Mappings with targets defined and that have at least one rule and should be mapped.
   */
  protected[loader] def mappingsWithRules =
    _mappings.filter(_.shouldMap).filter(_.trules.length > 0)

  /**
   *   Find mappings with invalid rulesets.
   */
  def mappingsWithDuplicativePriorities =
    mappingsWithRules.map { m =>
      val allPriorities = m.trules.map(_.priority)
      val allUniquePriorities = allPriorities.toSet
      if (allPriorities.size != allUniquePriorities.size)
        (true, m.target, "Duplicative priorities in rules. Rule prorities included: " + allPriorities.mkString(","))
      else
        (false, m.target, "No duplicative rules.")
    }.collect { case t if (t._1) => t }

  /** Unfold a stream and stop after emitting the first Error or Value. */
  def unfold[S](source: Seq[S], target: String)(f: S => RuleResult): scala.collection.immutable.Stream[RuleResult] = {
    import scala.collection.immutable.Stream
    def go(s: Seq[S]): scala.collection.immutable.Stream[RuleResult] =
      s.headOption match {
        case Some(head) =>
          try {
            f(head) match {
              case v: Value[_] => v #:: Stream.empty
              case na: NotApplicable => na #:: go(s.tail)
              case e: Error => e #:: Stream.empty
            }
          } catch {
            case scala.util.control.NonFatal(e) =>
              Error(s"Unable to process rules for target $target: ${e.getMessage}") #:: Stream.empty
          }
        case None => scala.collection.immutable.Stream.empty
      }
    go(source)
  }

  protected def _runRulesOn(ctx: RuleContext, mappings: Seq[Mapping[_]], f: Mapping[_] => Seq[Rule[_]]) = {
    val ruleResults = mappings.filterNot(_.skip).map { m =>
      val rules = f(m)
      val sortedrules = rules.sortBy(_.priority)
      //println(s"running rules for $m")
      val ruleResults = unfold(sortedrules, m.target)(_.f(ctx))

      val error = ruleResults.collect { case e: Error => e }.headOption
      val valueResult = ruleResults.collect { case v: Value[_] => v }.headOption
      val nas = ruleResults.collect { case na: NotApplicable => na }
      (m, valueResult, nas, error, rules.size)
    }
    //println(s"tranform:results: from applying the rules:\n$ruleResults")
    val valueMap = ruleResults.collect { case (m, Some(value), _, _, _) => (m.target, value.value) }.toMap[String, Option[_]]
    val errorMap = ruleResults.collect { case (m, _, _, Some(e), _) => (m.target, e) }.toMap[String, Error]
    val nasMap = ruleResults.collect { case (m, _, nas, _, numRules) if (nas.size == numRules) => (m.target, nas) }.toMap[String, Traversable[NotApplicable]]
    //println(s"transform result:\nvalues: $valueMap\nnas: $nasMap\nerrors: $errorMap")

    (valueMap, nasMap, errorMap)
  }

  /**
   * Run the transform part of the mapping using rules.
   * This function runs the logic specified in the rules for each
   * mapping for each input value where a mapping exists.
   *
   * @return Tuple of (1) successful transforms and their output values
   * indexed by the target name (if errors, this may or may not have
   * some partial results), (2) map of a mapping target to a list of
   * NotApplicables if a mapping *only* returned NotApplicables and (3) a map
   * of a mapping targets to an Error result if a mapping had an Error.
   */

  def transform(row: extractor.Record, idx: Int): (Map[String, Option[_]], Map[String, Traversable[NotApplicable]], Map[String, Error]) = {
    // Run the extractor...
    val extractor = self.extractor.StringMapExtractor()
    val econtext = self.extractor.RecordAndIdx(row, idx)
    extractor(econtext) match {
      case Validated.Valid((warnings, record)) =>
        //println(s"record is $record")
        _runRulesOn(BasicRuleContext(record), mappingsWithRules, _.trules)
      case Validated.Invalid(errors) =>
        //println("errors found")
        (Map.empty, Map.empty, errors.toList.map(e => (e.aname, Error(e.msg))).toMap)
    }
  }

  /**
   * Convert a map of optional values to their final loader
   *  formatted values. "Format" means changing the input value to
   *  objects needed by a target system. It as an error to ask
   *  for conversion but not have a loader function specified in the
   *  mapping object.
   *
   *  @param values Map of target names to optional values.
   *  @return May of target names to load-ready objects.
   */
  def load(values: Map[String, Option[_]]): Map[String, Parameter] = {
    //println(s"load with: $values")
    val lookup: Map[String, BasicMapping[_]] = mappingsWithRules.map(m => Tuple2(m.target, m)).toMap[String, BasicMapping[_]]
    val v = values.map {
      case (k, v) =>
        val tmp = lookup.get(k).map { m =>
          require(m.loader.isDefined) // filter out before?
          val pfunc = m.loader.get.asInstanceOf[Option[_] => Parameter]
          //println(s"creating loader for: $m: $v")
          val loaderValue = pfunc(v)
          (m.target, loaderValue)
        }
        tmp
    }
    v.collect { case Some(t) => t }.toMap
  }

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
