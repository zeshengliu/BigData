import java.util.Date
import java.util.Calendar
import java.text.SimpleDateFormat

private object StrArrayParam {
  def unapply(str: String): Option[Array[String]] = {
    Some(str.split(",").map(_.trim).toArray)
  }
}

class CommonArgs(args: Array[String]) {
  var cal = Calendar.getInstance
  cal.add(Calendar.DATE, -1)
  var date: Date = cal.getTime
  var bgnDate: Date = cal.getTime
  var userDate: Date = cal.getTime
  var dictDate: Date = cal.getTime

  var projectPath: String = "yanhan/sandbox"
  var hivePath: String = "yanhan/hivebox"
  var partitionCategory: String = "test"
  var featureDate: String = "test"
  var userBrowseDate: String = "test"
  var posId: String = "test"
  var skuinfoPath: String = "test"
  var userbasePath: String = "test"
  var guanxingPath: String = "test"
  var partData: String = "test"
  var interestDate: Date = cal.getTime
  var blcVecDate: Date = cal.getTime
  var recallBranch: String = "test"
  parse(args.toList)

  private def parse(args: List[String]): Unit = args match {
    case ("--date") :: value :: tail =>
      date = new SimpleDateFormat("yyyy-MM-dd").parse(value)
      parse(tail)

    case ("--bgn-date") :: value :: tail =>
      bgnDate = new SimpleDateFormat("yyyy-MM-dd").parse(value)
      parse(tail)

    case ("--project-path") :: value :: tail =>
      projectPath = value
      parse(tail)

    case ("--partition-category") :: value :: tail =>
      partitionCategory = value
      parse(tail)

    case("--feature-date") :: value :: tail =>
      featureDate = value
      parse(tail)

    case("--user-date") :: value :: tail =>
      userDate = new SimpleDateFormat("yyyy-MM-dd").parse(value)
      parse(tail)

    case ("--hive-path") :: value :: tail =>
      hivePath = value
      parse(tail)

    case ("--pos-id") :: value :: tail =>
      posId = value
      parse(tail)

    case ("--dict-date") :: value :: tail =>
      dictDate = new SimpleDateFormat("yyyy-MM-dd").parse(value)
      parse(tail)

    case ("--skuinfo-path") :: value :: tail =>
      skuinfoPath = value
      parse(tail)

    case ("--userbase-path") :: value :: tail =>
      userbasePath = value
      parse(tail)

    case ("--guanxing-path") :: value :: tail =>
      guanxingPath = value
      parse(tail)

    case ("--interest-date") :: value :: tail =>
      interestDate = new SimpleDateFormat("yyyy-MM-dd").parse(value)
      parse(tail)

    case ("--blc-vec-date") :: value :: tail =>
      blcVecDate = new SimpleDateFormat("yyyy-MM-dd").parse(value)
      parse(tail)

    case ("--recall-branch") :: value :: tail =>
      recallBranch = value
      parse(tail)

    case ("--part-data") :: value :: tail =>
      partData = value
      parse(tail)

    case ("--help") :: tail =>
      printUsageAndExit(0)

    case Nil =>

    case _ =>
      printUsageAndExit(1)
  }

  def printUsageAndExit(exitCode: Int) {
    System.err.println(
      "Usage: Worker [options]\n" +
        "\n" +
        "Options:\n" +
        "  --date DATE process date\n" +
        "  --project-path\n" +
        "  --hive-path\n" +
        "  --partition-category\n" +
        "\n")
    System.exit(exitCode)
  }
}