import scala.reflect.runtime.universe._

case class KafkaPlayerRecord(
    team_year: String,
    european: Boolean,
    african: Boolean,
    namerican: Boolean,
    samerican: Boolean,
    asian: Boolean,
    oceania: Boolean,
    potential: String,
    overall: String,
    pace:  String,
    defending:  String,
    shooting: String,
    passing: String,
    dribbling: String,
    physical: String,
    wages: String,
    country: String)