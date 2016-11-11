package org.im
package loader

import org.scalatest._
import cats._
import cats.implicits._
import cats.data._
import Validated._

class dslspec extends FlatSpec with Matchers {

  import Implicits._
  import com.lucidchart.open.relate.interp.Parameter._

  "A mapping" should "find all mappings" in {
    object tmaps extends mappings("a", "b") {
      import sourcefirst._
      string("f1").directMove
      string("f2").directMove
    }
    tmaps.mappings.length should equal(2)
  }

  it should "run simple source to target mappings" in {
    object tmaps extends mappings("a", "b") {
      import sourcefirst._
      string("sname").to("tname")
    }
    val (values, nas, errors) = tmaps.transform(Map("sname" -> Option("blah")), 1)
    assert(values.size == 1)
    values.get("tname").getOrElse(Some("")) shouldBe Some("blah")
  }

  it should "use a default value" in {
    object tmaps extends mappings("a", "b") {
      import sourcefirst._
      string("sname").to("tname").ifNotPresent("sname", Some("default value"))
    }
    val (values, nas, errors) = tmaps.transform(Map("sname" -> None), 1)
    withClue("values:") { assert(values.size == 1) }
    withClue("output value:") { values("tname") shouldBe Some("default value") }
  }

  it should "always set the value to a constant when requested" in {
    object tmaps extends mappings("a", "b") {
      import sourcefirst._
      long("iattr").to_("tattr").constant(Some(1))
    }
    val (values, nas, errors) = tmaps.transform(Map("iattr" -> Some("30")), 1)
    withClue("values:") { assert(values.size == 1) }
    withClue("output value:") { values("tattr") shouldBe Some(1) }
  }

  it should "always map to None using mapToNoValue" in {
    object tmaps extends mappings("a", "b") {
      import sourcefirst._
      string("sname").to("tname").mapToNoValue
    }
    val (values, nas, errors) = tmaps.transform(Map("sname" -> Some("crazy value")), 1)
    assert(values.size == 1)
    values.get("tname").get shouldBe None
  }

  it should "map to the same column automatically if only the source was given for some typed combinators" in {
    object tmaps extends mappings("a", "b") {
      import sourcefirst._
      long("sname").directMove
    }
    val (values, nas, errors) = tmaps.transform(Map("sname" -> Some("100")), 1)
    assert(values.size == 1)
    assert(values.contains("sname"))
    values("sname").getOrElse(0) shouldBe (100)
  }

  it should "convert string boolean to an integer" in {
    object tmaps extends mappings("a", "b") {
      import sourcefirst._
      strbool("str2bool").directMove
    }
    val (values, nas, errors) = tmaps.transform(Map("str2bool" -> Some("true")), 1)
    assert(values.size == 1)
    assert(values.contains("str2bool"))
    values("str2bool").getOrElse(-1) shouldBe (1)
  }

  it should "ignore a mapping marked as ignore" in {
    object tmaps extends mappings("a", "b") {
      import sourcefirst._
      string("cola").ignore
    }
    tmaps.mappings.length shouldBe (0)
    tmaps.allMappings.length shouldBe (1)
  }

  it should "return target column names" in {
    object tmaps extends mappings("a", "b") {
      import sourcefirst._
      string("cola").directMove
      string("colb").to("targetb")
    }
    tmaps.targetCols should contain inOrderOnly ("cola", "targetb")
  }

  it should "not screw up the source and target if only the target is specified and there are rules" in {
    object tmaps extends mappings("a", "b") {
      import sourcefirst._
      to[String]("cola")
      to[String]("colb").rule { ctx => ctx.none }
    }
    tmaps.mappings.find(_.target == "cola").map(_.target) shouldBe None
    tmaps.mappings.find(_.target == "colb").map(_.target) shouldBe Some("colb")
  }

  it should "not create multiple mappings when using combinators" in {
    object tmaps extends mappings("a", "b") {
      import sourcefirst._
      string("a").to("a")
      to[String]("cola").source("colasource").rule { ctx => ctx.none }
    }
    tmaps.mappings.size shouldBe (2)
    tmaps.allMappings.size shouldBe (2)
  }

  "Mappings" should "detect mappings with duplicative priority rules" in {
    val result = try {
      object tmaps extends mappings("a", "b") {
        to[String]("cola").rule { ctx =>
          ctx.success("a")
        }
        ruleFor[String, String]("cola")(0) { ctx =>
          ctx.none
        }
      }
      fail("Did not detect duplicative priorities.")
    } catch {
      case scala.util.control.NonFatal(_) => succeed
    }
    result
  }

  it should "contain rules that return success/none/failure" in {
    object tmaps extends mappings("a", "b") {
      to[String]("cola").rule { ctx =>
        ctx.success("a")
      }
      to[String]("colb").rule { ctx =>
        import ctx._
        none
      }
      to[String]("colc").rule { ctx =>
        ctx.notApplicable
      }
      to[String]("theerrortarget").rule { ctx =>
        ctx.error("Could not find source data.")
      }
    }
    val (values, nas, errors) = tmaps.transform(Map(), 1)
    withClue("nas targets") { nas.size shouldBe (1) }
    withClue("error targets") { errors.size shouldBe (1) }
    withClue("# valid results") { values.size shouldBe (2) }
  }

  it should "fire rules in order and stop when a result is found" in {
    object tmaps extends mappings("a", "b") {
      to[String]("cola").
        rule { ctx =>
          ctx.notApplicable
        }.
        rule { ctx =>
          ctx.notApplicable
        }.
        rule { ctx =>
          ctx.success("a")
        }
      ruleFor[String, String]("cola")(3) { ctx =>
        ctx.error("You should never reach this rule.")
      }

    }
    val (values, nas, errors) = tmaps.transform(Map(), 1)
    withClue("values") { values.size shouldBe (1) }
    withClue("errors") { errors.size shouldBe (0) }
    withClue("nas") { nas.size shouldBe (0) }
  }

  import Implicits._
  import com.lucidchart.open.relate.interp._

  object tmaps1 extends mappings("a", "b") {
    import sourcefirst._
    to[String]("cola").
      rule { ctx =>
        import ctx._
        input.get("colasource") match {
          case Some(v) => success(v)
          case _ => none
        }
      }

    int("colbsource").to("colb")
    float("colcsource").to("colc")
    int("cold").directMove
  }

  "Transform" should "return transform results correctly" in {
    val (values, nas, errors) = tmaps1.transform(Map(
      "colasource" -> Option("colavalue"),
      "colbsource" -> Option("100"),
      "colcsource" -> Option("12.5"),
      "cold" -> Option("1")), 1)

    withClue("nas targets") { nas.size shouldBe (0) }
    withClue("error targets") { errors.size shouldBe (0) }
    withClue("# valid results") { values.size shouldBe (4) }

    withClue("cola") {
      values("cola") shouldBe Some("colavalue")
    }
    withClue("colb") {
      values("colb") shouldBe Some(100)
    }
    withClue("colc") {
      values("colc") shouldBe Some(12.5)
    }
    withClue("cold") {
      values("cold") shouldBe Some(1)
    }
  }

  it should "require a source attribute if its called out in a mapping with an extractor" in {
    val (values, nas, errors) = tmaps1.transform(Map(
      "colasource" -> Option("colavalue"),
      "colbsource" -> Option("100"),
      "colcsource" -> Option("12.5")), 1)

    withClue("error targets") { errors.size shouldBe (1) }
    withClue("nas targets") { nas.size shouldBe (0) }
    withClue("# valid results") { values.size shouldBe (0) }
  }

  it should "return target system loader objects correctly" in {
    val (values, nas, errors) = tmaps1.transform(Map(
      "colasource" -> Option("colavalue"),
      "colbsource" -> Option("100"),
      "colcsource" -> Option("12.5"),
      "cold" -> Option("10")), 1)

    values.size shouldBe (4)
    errors.size shouldBe (0)
    nas.size shouldBe (0)

    val loaders = tmaps1.load(values)
    withClue("length") { loaders.size shouldBe (4) }
    withClue("loader object types") {
      assert(loaders("cola").isInstanceOf[StringParameter])
      assert(loaders("colb").isInstanceOf[IntParameter])
      assert(loaders("colc").isInstanceOf[FloatParameter])
    }
    val arglist = sequenceLikeUnsafe(Seq("colb", "colc", "cola"), loaders)
    withClue("arglist order") {
      arglist.length shouldBe (3)
      assert(arglist(2).isInstanceOf[StringParameter])
      assert(arglist(0).isInstanceOf[IntParameter])
      assert(arglist(1).isInstanceOf[FloatParameter])
    }
  }

  it should "inline dates, by default, should error if unable to convert, or NA if specified" in {
    object tmapsdates extends mappings("a", "b") {
      import sourcefirst._
      date("dcol").to("target").required
      date("dcol2").to("target2").required
      date("dcol3", true).to("target3").required
    }

    val (values, nas, errors) = tmapsdates.transform(Map(
      "dcol" -> Some("2016-01-01T01:01:01"),
      "dcol2" -> Some("2016-01-01T01:01:01Z"),
      "dcol3" -> Some("2016-01-01T01:01:01")), 1)

    // 2 because dcol3 date() will be NA then a direct move rule runs last
    withClue("values:") { values.size shouldBe (2) }
    withClue("errors:") { errors.size shouldBe (1) }
    withClue("nas:") { nas.size shouldBe (0) }
  }

  it should "allow follow on rules for dates" in {
    val fallback = inputToDate(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME) _

    object tmapsdates extends mappings("a", "b") {
      import sourcefirst._
      date("d").to("target").rule { ctx =>
        import ctx._
        toRuleResultC(false)(fallback(input.getAs[String]("d")))
      }
    }
    val (values, nas, errors) = tmapsdates.transform(Map("d" -> Some("2016-01-01T01:01:01")), 1)
    values.size shouldBe (1)
    errors.size shouldBe (0)
    nas.size shouldBe (0)
  }

  it should "transform a long correctly" in {
    object test1 extends mappings("a", "b") {
      import sourcefirst._
      long("cola").to("colavalue")
    }
    val (values, nas, errors) = test1.transform(Map("cola" -> Some("100")), 1)
    values.size shouldBe (1)
    errors.size shouldBe (0)
    nas.size shouldBe (0)

    values("colavalue") shouldBe Some(100)
  }

  "schema" should "capture input conversions and handle dynamic values correctly" in {
    object test1 extends mappings("a", "b") {
      override object ischema extends schema {
        slong("colasource")
        sdouble("colbsource")
      }

      to[String]("colatarget").rule(0) { ctx =>
        import ctx._
        val newValue = input.getAs[Long]("colasource").map(_ + 30.0) getOrElse -1.0
        success(newValue.toString + "!")
      }

      to[Double]("colbtarget").rule(0) { ctx =>
        import ctx._
        ctx.colbsource[Double].filter(_ < 2.0).fold[RuleResult](ctx.notApplicable)(v => ctx.success(v + 100.0))
      }.rule(1) { ctx =>
        ctx.success(ctx.colbsource[Double].map(_ + 30.0) getOrElse -1.0)
      }
    }

    val (values, nas, errors) = test1.transform(Map(
      "colasource" -> Option("3"),
      "colbsource" -> Option("4.0")), 1)
    values.size shouldBe (2)
    errors.size shouldBe (0)
    nas.size shouldBe (0)
    values("colatarget") shouldBe Some("33.0!")
    values("colbtarget") shouldBe Some(34.0)
  }

  it should "allow the use of the schema dsl with rule1" in {
    object test1 extends mappings("a", "b") {
      override object ischema extends schema {
        slong("colasource").required
        sfloat("colbsource").required
        slong("colcsource")
      }
      to[Long]("colatarget").rule1[Long](0)("colasource") { (ctx, v) =>
        ctx.success(v)
      }
      to[Float]("colbtarget").rule1[Float](0)("colbsource") { (ctx, v) =>
        ctx.success(v)
      }
      to[Long]("colctarget").fromX[Long]("colcsource")

    }
    val (values, nas, errors) = test1.transform(Map(
      "colasource" -> Option("3"),
      "colbsource" -> Option("4.0"),
      "colcsource" -> Option("100")), 1)
    values.size shouldBe (3)
    errors.size shouldBe (0)
    nas.size shouldBe (0)
    values("colatarget") shouldBe Some(3)
    values("colbtarget") shouldBe Some(4.0)
    values("colctarget") shouldBe Some(100)
  }

  it should "return simple conversion errors if the input does not match" in {
    object test1 extends mappings("a", "b") {
      override object ischema extends schema {
        sdate("datesource")
      }
      to[java.sql.Date]("datetarget").rule1[java.sql.Date](0)("datesource") { (ctx, v) =>
        ctx.success(v)
      }
    }
    val (values, nas, errors) = test1.transform(Map(
      "datesource" -> Option("3")), 1)
    //println(s"$values\n$nas\n$errors")
    values.size shouldBe (0)
    errors.size shouldBe (1)
    nas.size shouldBe (0)
  }

}
