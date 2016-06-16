package local

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.FunSuite
import sw.air.{Airline, Airport, Route}

class SQLTest extends FunSuite with SharedSparkContext {
  override val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "true").
    set("spark.app.id", appID)
  private var sqlCtx: SQLContext = null
  private var airports: DataFrame = null
  private var airlines: DataFrame = null
  private var routes: DataFrame = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlCtx = new SQLContext(sc)
    airports = Airport(sqlCtx)
    airlines = Airline(sqlCtx)
    routes = Route(sqlCtx)
    airports.registerTempTable("airports")
    airlines.registerTempTable("airlines")
    routes.registerTempTable("routes")
  }

  test("airport geo") {
    val geoSQL =
      """
       |SELECT * FROM airports
       |WHERE (latitude BETWEEN -11 AND 39)
       |  AND (longitude BETWEEN 37 AND 70)
      """.stripMargin
    sqlCtx.sql(geoSQL).show()
  }

  test("Inactive airlines in countries") {
    val inactiveSQL =
      """
       |SELECT * FROM airlines
       |WHERE active = 'N' AND country IN ('United Kingdom', 'Germany', 'France')
      """.stripMargin
    sqlCtx.sql(inactiveSQL).show()
  }
  test("Num of airport") {
    val countrySQL =
      """
       |SELECT COUNT(*), country FROM airports
       |GROUP BY country
       |
      """.stripMargin
    sqlCtx.sql(countrySQL).show()
  }

  test("Ideal spot in country which is ideally located to all airports") {
    val placeSQL =
      """
       | SELECT country, AVG(latitude), AVG(longitude)
       | FROM airports
       | GROUP BY country
      """.stripMargin
    sqlCtx.sql(placeSQL).show()
  }
  test("Display routes with airport names") {
    val routeSQL =
      """
       |SELECT l.name, r.sourceAirport, sa.name, r.destinationAirport, da.name
       |FROM routes AS r
       |JOIN airlines AS l ON r.airlineId = l.id
       |JOIN airports AS sa ON r.sourceAirportId = sa.id
       |JOIN airports AS da ON r.destinatationAirportId = da.id
      """.stripMargin
    sqlCtx.sql(routeSQL).show()
  }

  test("Num of airports in each country sorted by count") {
    import org.apache.spark.sql.functions._
    airports.groupBy("country").count().distinct().orderBy(col("count").desc).show()
    val distintcSQL =
      """
       |SELECT country, COUNT(country) AS c
       |FROM airports
       |GROUP BY country
       |ORDER BY c DESC
      """.stripMargin
    sqlCtx.sql(distintcSQL).show()
  }

  test("Find all mails from Kenneth Lay") {
    val enron = sqlCtx.read.json("src/main/resources/enron/enron.json")
    import org.apache.spark.sql.functions._
    enron.where(col("sender") === "kenneth.lay@enron.com").select("sender", "subject").show(false)
  }

  test("Find all mails where Kenneth Lay was BCCed") {
    val enron = sqlCtx.read.json("src/main/resources/enron/enron.json")
    import org.apache.spark.sql.functions._
    enron.filter(array_contains(col("bcc"), "kenneth.lay@enron.com")).select("sender", "bcc").show(false)
  }

}
