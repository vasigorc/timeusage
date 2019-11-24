package timeusage

import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col
import org.junit.runner.RunWith
import org.scalatest
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import scala.language.{existentials, higherKinds, postfixOps}
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {

  import TimeUsage._
  import spark.implicits._

  org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)

  private val readData = scalatest.Tag("Read-in Data")
  private val project = scalatest.Tag("Project")
  private val inputList = List(Atussum("John", 2d, 1d, 22d), Atussum("Jean-Pierre", 4d, 1d, 28d),
    Atussum("Marie", 1d, 2d, 40d), Atussum("Manon", 4d, 2d, 40d),
    Atussum("Charles", 2d, 1d, 15d))

  case class Atussum(name: String,
                     telfs: Double,
                     tesex: Double,
                     teage: Double,
                     t1801: Double = Random.nextDouble(),
                     t1803: Double = Random.nextDouble(),
                     t05: Double = Random.nextDouble(),
                     t1805: Double = Random.nextDouble(),
                     t10: Double = Random.nextDouble(),
                     t12: Double = Random.nextDouble())

  private val atussumOccupationalColumnHeaders = List("t1801", "t1803", "t05", "t1805", "t10", "t12")
  var df: DataFrame = _

  before {
    df = inputList.toDF
  }

  test("`dfSchema` produces right schema based on input", readData) {
    val header =
      """
        tucaseid,gemetsta,gtmetsta,peeduca,pehspnon,ptdtrace,teage,telfs,temjot,teschenr,teschlvl,tesex,
        tespempnot,trchildnum,trdpftpt,trernwa,trholiday,trspftpt,trsppres,tryhhchild,tudiaryday,tufnwgtp,
        tehruslt,tuyear""".split(",").toList

    val schema = dfSchema(header)
    assert(schema.size == header.size, "# of StructFields must be equal to # of columns in initial header")

    val fieldNames = schema.fieldNames

    assert(header.containsAll(fieldNames.toList), "All fields from the raw header should have been imported")
  }

  test("`row` generates a Row of (String, Double*) from the passed-in list of Strings", readData) {
    val validInput = List("Vasile", "0.1", "0.2", "0.3", "0.4")

    val result = row(validInput)

    assert(validInput.size == result.length, "Row should include same # of columns as validInput")

    assert(result.getString(0) == "Vasile", "First value should be the provided String")
    assert(result.getDouble(3) == 0.3d, "Third element should be double equivalent of input's String at the " +
      "same index")
  }

  private def generateColumnNamesFromPrefixes(primaryNeedsPrefixes: List[String]): List[String] = {
    primaryNeedsPrefixes filter (_ nonEmpty) map (_ + randomStringOf())
  }

  private def randomStringOf(length: Int = 4): String = {
    RandomStringUtils.randomAlphanumeric(length)
  }

  test("`classifiedColumns`should triage column names into three groups of columns", project) {
    val primaryNeeds: List[String] = generateColumnNamesFromPrefixes(primaryNeedsPrefixes)
    val workingActivities: List[String] = generateColumnNamesFromPrefixes(workingActivitiesPrefixes)
    val otherActivities: List[String] = generateColumnNamesFromPrefixes(otherActivitiesPrefixes)

    val allActivities: List[String] = Random.shuffle(primaryNeeds ::: workingActivities ::: otherActivities)

    val (primaryColumns, workingColumns, otherColumns) = classifiedColumns(allActivities)

    assert(primaryNeeds.map(col) containsAll primaryColumns, "all primary columns should be found")
    assert(workingActivities.map(col) containsAll workingColumns, "all working columns should be found")
    assert(otherActivities.map(col) containsAll otherColumns, "all other columns should be found")
  }

  test("`timeUsageSummary` should combine occupational columns", project) {
    val (primaryColumns, workingColumns, otherColumns) = classifiedColumns(atussumOccupationalColumnHeaders)

    val generalizedDf = timeUsageSummary(primaryColumns, workingColumns, otherColumns, df)
    // primary needs

    val john = inputList.head

    // primary needs
    val expectedPrimaryNeedsTime = (john.t1801 + john.t1803) / 60
    val actualPrimaryNeedsTime = getDoubleValueForColumn(generalizedDf, 'primaryNeeds, 'name === "John")
    assert(expectedPrimaryNeedsTime == actualPrimaryNeedsTime, "primary needs time columns should be summed")

    // working time
    val expectedWorkingTime = (john.t05 + john.t1805) / 60
    val actualWorkingTime = getDoubleValueForColumn(generalizedDf, 'work, 'name === "John")
    assert(expectedWorkingTime == actualWorkingTime, "working time columns should be summed")

    // other time
    val expectedOtherTime = (john.t10 + john.t12) / 60
    val actualOtherTime = getDoubleValueForColumn(generalizedDf, 'other, 'name === "John")
    assert(expectedOtherTime == actualOtherTime, "other time columns should be summed")
  }

  test("`timeUsageSummary` should generalize work status, sex and age information", project) {
    val (primaryColumns, workingColumns, otherColumns) = classifiedColumns(atussumOccupationalColumnHeaders)

    val generalizedDf = timeUsageSummary(primaryColumns, workingColumns, otherColumns, df)

    val expectedNrOfWomen = inputList.count(_.tesex == 2d)
    val actualNrOfWomen = generalizedDf.where('sex === "female").count()
    assert(expectedNrOfWomen == actualNrOfWomen, s"There should be $expectedNrOfWomen of women in the DF")
  }

  private def getDoubleValueForColumn(generalizedDf: DataFrame, column: Column, filter: Column) = {
    generalizedDf.select(column).where(filter).first().getDouble(0)
  }

  implicit class SeqContainsAll[T, SeqAndBelow[+T] <: Seq[T]](self: SeqAndBelow[T]) {
    def containsAll(that: SeqAndBelow[T]): Boolean = self forall (that contains)
  }

}