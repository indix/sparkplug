package sparkplug

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{
  AccumulableInfo,
  SparkListener,
  SparkListenerJobEnd,
  SparkListenerStageCompleted
}
import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import sparkplug.models.{
  PlugAction,
  PlugDetail,
  PlugRule,
  PlugRuleValidationError
}

import scala.concurrent.{Future, Promise}

case class TestRow(title: String, brand: String, price: Int)
case class TestRowWithPlugDetails(title: String,
                                  brand: String,
                                  price: Int,
                                  plugDetails: Seq[PlugDetail] = Seq())
case class TestRowWithOverrideDetails(title: String,
                                      brand: String,
                                      price: Int,
                                      overrideDetails: Seq[PlugDetail] = Seq())
case class TestPriceDetails(minPrice: Double,
                            maxPrice: Double,
                            availability: String = "available")
case class TestRowWithStruct(title: String,
                             brand: String,
                             price: Option[TestPriceDetails])

trait SpecAccumulatorsSparkListener extends ScalaFutures {

  implicit val spark: SparkSession

  def addListener: Future[Map[String, Long]] = {
    val promise = Promise[Map[String, Long]]()

    spark.sparkContext.addSparkListener(new SparkListener {
      val accumulatorsNamespace = "SparkPlug"
      var accumulators = Map[String, Long]()

      override def onStageCompleted(
          stageCompleted: SparkListenerStageCompleted) {
        stageCompleted.stageInfo.accumulables.foreach {
          case (_, info: AccumulableInfo) =>
            info.name
              .filter(_.startsWith(accumulatorsNamespace))
              .foreach((s: String) => {
                accumulators = accumulators ++ Map(
                  s -> info.value.get.asInstanceOf[Long])
              })
        }
      }

      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        if (!promise.isCompleted) promise success accumulators
      }
    })

    promise.future
  }
}

class SparkPlugSpec
    extends FlatSpec
    with Matchers
    with SpecAccumulatorsSparkListener {
  implicit val spark: SparkSession = SparkSession.builder
    .config(new SparkConf())
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  "SparkPlug" should "return input df as is" in {
    val df = spark.emptyDataFrame
    val sparkPlug = SparkPlug.builder.create()
    sparkPlug.plug(df, List.empty).right.get should be(df)
  }

  it should "add default plug details to the df if enabled" in {
    val df = spark.emptyDataFrame
    val sparkPlug = SparkPlug.builder.enablePlugDetails().create()
    sparkPlug.plug(df, List.empty).right.get.schema.fieldNames should contain(
      "plugDetails")
  }

  it should "add custom plug details to the df if enabled" in {
    val df = spark.emptyDataFrame
    val sparkPlug =
      SparkPlug.builder.enablePlugDetails("overrideDetails").create()
    sparkPlug.plug(df, List.empty).right.get.schema.fieldNames should contain(
      "overrideDetails")
  }

  it should "validate rules if enabled" in {
    val df = spark.createDataFrame(
      List(
        TestRow("iPhone", "Apple", 300),
        TestRow("Galaxy", "Samsung", 200)
      ))
    val sparkPlug = SparkPlug.builder.enableRulesValidation.create()
    val invalidRules = List(
      PlugRule("rule1",
               "version1",
               "title like '%iPhone%'",
               Seq(PlugAction("randomField", "1"))),
      PlugRule("rule2",
               "version1",
               "title like '%iPhone%'",
               Seq(PlugAction("price", "too high")))
    )
    sparkPlug.plug(df, invalidRules).left.get should be(
      List(
        PlugRuleValidationError(
          "rule1",
          "Field \"randomField\" not found in the schema."),
        PlugRuleValidationError(
          "rule2",
          "Value \"too high\" cannot be assigned to field price.")
      ))
  }

  it should "validate only one version of rule applied" in {
    val df = spark.createDataFrame(
      List(
        TestRow("iPhone", "Apple", 300),
        TestRow("Galaxy", "Samsung", 200)
      ))
    val sparkPlug = SparkPlug.builder.enableRulesValidation.create()
    val invalidRules = List(
      PlugRule("rule1",
               "version1",
               "title like '%iPhone%'",
               Seq(PlugAction("randomField", "1"))),
      PlugRule("rule1",
               "version2",
               "title like '%iPhone%'",
               Seq(PlugAction("price", "too high")))
    )
    sparkPlug.plug(df, invalidRules).left.get should be(
      List(
        PlugRuleValidationError(
          "rule1",
          "Only one version per rule should be applied."),
        PlugRuleValidationError(
          "rule1",
          "Field \"randomField\" not found in the schema."),
        PlugRuleValidationError(
          "rule1",
          "Value \"too high\" cannot be assigned to field price.")
      ))
  }

  it should "apply rules" in {
    val df = spark.createDataFrame(
      List(
        TestRow("iPhone", "Apple", 300),
        TestRow("Galaxy", "Samsung", 200)
      ))
    val sparkPlug = SparkPlug.builder.create()
    val rules = List(
      PlugRule("rule1",
               "version1",
               "title like '%iPhone%'",
               Seq(PlugAction("price", "1000"),
                   PlugAction("title", "Apple iPhone"))),
      PlugRule("rule2",
               "version1",
               "title like '%Galaxy%'",
               Seq(PlugAction("price", "700"),
                   PlugAction("title", "Apple iPhone")))
    )

    import spark.implicits._
    val output = sparkPlug.plug(df, rules).right.get.as[TestRow].collect()
    output.length should be(2)
    output(0).price should be(1000)
    output(1).price should be(700)
  }

  it should "checkpoint" in {
    val df = spark.createDataFrame(
      List(
        TestRow("iPhone", "Apple", 300),
        TestRow("Galaxy", "Samsung", 200)
      ))
    val sparkPlug = SparkPlug.builder
      .enableCheckpointing("src/test/resources/checkpoint_dir", 1, 10)
      .create()
    val rules = List(
      PlugRule("rule1",
               "version1",
               "title like '%iPhone%'",
               Seq(PlugAction("price", "1000"))),
      PlugRule("rule2",
               "version1",
               "title like '%Galaxy%'",
               Seq(PlugAction("price", "700")))
    )

    import spark.implicits._
    val output = sparkPlug.plug(df, rules).right.get.as[TestRow].collect()
    output.length should be(2)
    output.filter(_.title == "iPhone").head.price should be(1000)
    output.filter(_.title == "Galaxy").head.price should be(700)
  }

  it should "set accumulators" in {
    val df = spark.createDataFrame(
      List(
        TestRow("iPhone", "Apple", 300),
        TestRow("Galaxy", "Samsung", 200)
      ))
    val sparkPlug = SparkPlug.builder.enableAccumulators
      .create()
    val rules = List(
      PlugRule("rule1",
               "version1",
               "title like '%iPhone%'",
               Seq(PlugAction("price", "1000"))),
      PlugRule("rule2",
               "version1",
               "title like '%Galaxy%'",
               Seq(PlugAction("price", "700"))),
      PlugRule("rule3",
               "version1",
               "title like '%Galaxy%'",
               Seq(PlugAction("price", "700")))
    )

    import spark.implicits._
    val accumulator = addListener
    val output = sparkPlug.plug(df, rules).right.get.as[TestRow].collect()
    output.length should be(2)
    accumulator.futureValue should be(
      Map(
        "SparkPlug.Changed" -> 2
      ))
  }

  it should "be able to validate derived values" in {
    val df = spark.createDataFrame(List.empty[TestRow])
    val sparkPlug = SparkPlug.builder.enableRulesValidation.create()
    val rules = List(
      PlugRule("rule1",
               "version1",
               "true",
               Seq(PlugAction("title", "`conc(brand, ' ', title)`")))
    )

    val errors = sparkPlug.plug(df, rules).left.get
    errors.length should be(1)
    errors.head.error should startWith(
      "[SQL Error] Undefined function: 'conc'")
  }

  it should "be able to set derived values" in {
    val df = spark.createDataFrame(
      List(
        TestRowWithStruct("iPhone",
                          "Apple",
                          Some(TestPriceDetails(100.0, 150.0))),
        TestRowWithStruct("Galaxy",
                          "Samsung",
                          Some(TestPriceDetails(10.0, 15.0, "not available"))),
        TestRowWithStruct("Lumia", "Nokia", None)
      ))
    val sparkPlug = SparkPlug.builder.create()
    val rules = List(
      PlugRule("rule1",
               "version1",
               "true",
               Seq(PlugAction("title", "`concat(brand, ' ', title)`")))
    )

    import spark.implicits._
    val output =
      sparkPlug.plug(df, rules).right.get.as[TestRowWithStruct].collect()
    output.length should be(3)
    output.map(_.title) should contain inOrderElementsOf List("Apple iPhone",
                                                              "Samsung Galaxy",
                                                              "Nokia Lumia")
  }

  it should "apply rules to struct fields" in {
    val df = spark.createDataFrame(
      List(
        TestRowWithStruct("iPhone",
                          "Apple",
                          Some(TestPriceDetails(100.0, 150.0))),
        TestRowWithStruct("Galaxy",
                          "Samsung",
                          Some(TestPriceDetails(10.0, 15.0, "not available"))),
        TestRowWithStruct("Lumia", "Nokia", None)
      ))
    val sparkPlug = SparkPlug.builder.create()
    val rules = List(
      PlugRule("rule1",
               "version1",
               "title like '%iPhone%'",
               Seq(PlugAction("price.minPrice", "1000.0"),
                   PlugAction("title", "Apple iPhone"))),
      PlugRule("rule2",
               "version1",
               "title like '%Galaxy%'",
               Seq(PlugAction("price.availability", "available"))),
      PlugRule("rule3",
               "version1",
               "title like '%Lumia%'",
               Seq(PlugAction("price.availability", "available")))
    )

    import spark.implicits._
    val output =
      sparkPlug.plug(df, rules).right.get.as[TestRowWithStruct].collect()
    output.length should be(3)
    output(0).price.get.minPrice should be(1000.0)
    output(0).title should be("Apple iPhone")
    output(1).price.get.availability should be("available")
    output(2).price.isDefined should be(false)
  }

  it should "apply rules with plug details" in {
    val df = spark.createDataFrame(
      List(
        TestRowWithPlugDetails("iPhone", "Apple", 300),
        TestRowWithPlugDetails("Galaxy", "Samsung", 200)
      ))
    val sparkPlug = SparkPlug.builder.enablePlugDetails().create()
    val rules = List(
      PlugRule("rule1",
               "version1",
               "title like '%iPhone%'",
               Seq(PlugAction("price", "1000"))),
      PlugRule("rule2",
               "version1",
               "title like '%Galaxy%'",
               Seq(PlugAction("price", "700")))
    )

    import spark.implicits._
    val output =
      sparkPlug.plug(df, rules).right.get.as[TestRowWithPlugDetails].collect()
    output.length should be(2)
    output(0).price should be(1000)
    output(0).plugDetails should be(
      Seq(PlugDetail("rule1", "version1", Seq("price"))))

    output(1).price should be(700)
    output(1).plugDetails should be(
      Seq(PlugDetail("rule2", "version1", Seq("price"))))
  }

  it should "apply rules with custom plug details" in {
    val df = spark.createDataFrame(
      List(
        TestRowWithPlugDetails("iPhone", "Apple", 300),
        TestRowWithPlugDetails("Galaxy", "Samsung", 200)
      ))
    val sparkPlug =
      SparkPlug.builder.enablePlugDetails("overrideDetails").create()
    val rules = List(
      PlugRule("rule1",
               "version1",
               "title like '%iPhone%'",
               Seq(PlugAction("price", "1000"))),
      PlugRule("rule2",
               "version1",
               "title like '%Galaxy%'",
               Seq(PlugAction("price", "700")))
    )

    import spark.implicits._
    val output =
      sparkPlug
        .plug(df, rules)
        .right
        .get
        .as[TestRowWithOverrideDetails]
        .collect()
    output.length should be(2)
    output(0).price should be(1000)
    output(0).overrideDetails should be(
      Seq(PlugDetail("rule1", "version1", Seq("price"))))

    output(1).price should be(700)
    output(1).overrideDetails should be(
      Seq(PlugDetail("rule2", "version1", Seq("price"))))
  }

  it should "apply rules with plug details even if not in input" in {
    val df = spark.createDataFrame(
      List(
        TestRow("iPhone", "Apple", 300),
        TestRow("Galaxy", "Samsung", 200)
      ))
    val sparkPlug = SparkPlug.builder.enablePlugDetails().create()
    val rules = List(
      PlugRule("rule1",
               "version1",
               "title like '%iPhone%'",
               Seq(PlugAction("price", "1000"))),
      PlugRule("rule2",
               "version1",
               "title like '%Galaxy%'",
               Seq(PlugAction("price", "700")))
    )

    import spark.implicits._
    val output =
      sparkPlug.plug(df, rules).right.get.as[TestRowWithPlugDetails].collect()
    output.length should be(2)
    output(0).price should be(1000)
    output(0).plugDetails should be(
      Seq(PlugDetail("rule1", "version1", Seq("price"))))

    output(1).price should be(700)
    output(1).plugDetails should be(
      Seq(PlugDetail("rule2", "version1", Seq("price"))))
  }
}
