package edu.ucsc.sig_server

import java.io.StringWriter

import org.eclipse.jetty.server.Server
import org.rogach.scallop

import org.eclipse.jetty.webapp.WebAppContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import javax.servlet.http.{HttpServletRequest, HttpServletResponse, HttpServlet}
import org.apache.spark.storage.StorageLevel

import javax.script.ScriptEngine
import collection.JavaConverters._
import com.fasterxml.jackson.databind.{SerializerProvider, JsonSerializer, ObjectMapper}
import com.fasterxml.jackson.core.{JsonGenerator, JsonFactory}
import com.fasterxml.jackson.databind.module.SimpleModule

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation

import org.saddle.{Vec, Series}

object JettyLauncher {

  var sc : SparkContext = null
  var engine : ScriptEngine = null

  val JSON_MAPPER = new ObjectMapper()

  val JSON_FACTORY = new JsonFactory(JSON_MAPPER)
  JSON_FACTORY.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
  JSON_FACTORY.disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
  JSON_FACTORY.disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)

  val JSON_MODULE = new SimpleModule()
  JSON_MODULE.addSerializer( new JsonSerializer[Map[String,AnyRef]] {
    override def serialize(t: Map[String,AnyRef], jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider) = {
      jsonGenerator.writeObject(t.asJava)
    }
    override def handledType() = classOf[Map[String,AnyRef]]
  })
  /*
  JSON_MODULE.addSerializer( new JsonSerializer[SparkGraphElementBase] {
    override def serialize(value: SparkGraphElementBase, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
      jgen.writeStartObject()
      jgen.writeNumberField("id", value.getID())
      jgen.writeObjectField("properties", value.getPropertyKeys().asScala.map( x => (x, value.getProperty(x))).toMap.asJava )
      jgen.writeEndObject()
    }
    override def handledType() = classOf[SparkGraphElementBase]
  })
  */
  JSON_MAPPER.registerModule(JSON_MODULE)

  var model_db : RDD[(String, Series[String,Double], java.util.Map[String,AnyRef])] = null

  def main(args: Array[String]) {

    object cmdline extends scallop.ScallopConf(args) {
      val spark: scallop.ScallopOption[String] = opt[String]("spark", default = Option("local"))
      val port: scallop.ScallopOption[Int] = opt[Int]("port", default=Option(8080))
      val models = trailArg[String]()
    }

    val conf = new SparkConf().setMaster(cmdline.spark()).setAppName("SigServer")

    //load and cache the data
    sc = new SparkContext(conf)

    model_db = sc.textFile(cmdline.models()).map( x => {
      new ObjectMapper().readValue(x, classOf[java.util.Map[String,AnyRef]])
    }).filter(_.containsKey("coef")).map( x => {
      val coef = x.get("coef").asInstanceOf[java.util.Map[String,Double]].asScala.toList
      (x.get("label").asInstanceOf[String], Series( coef :_* ), x.get("params").asInstanceOf[java.util.Map[String,AnyRef]])
    }).coalesce(100).persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("Database Size: " + model_db.count())
    //setup the server
    val server = new Server(cmdline.port())
    val context = new WebAppContext()
    context.setContextPath("/")
    context.setResourceBase("src/main/webapp")

    class RootServlet extends HttpServlet {
      override def doGet(request: HttpServletRequest, response: HttpServletResponse ) = {
        response.setContentType("text/html")
        response.setStatus(HttpServletResponse.SC_OK)
        response.getWriter().println("DBSize:" + model_db.count)
      }
    }

    class SignatureSearch extends HttpServlet {
      override def doPost(request: HttpServletRequest, response: HttpServletResponse ) = {
        response.setContentType("text/html")
        response.setStatus(HttpServletResponse.SC_OK)
        val sig = request.getReader.readLine()
        val overlap_a = request.getParameterValues("minoverlap")
        val overlap = if (overlap_a.length > 0) {
          overlap_a(0).toInt
        } else {
          0L
        }

        val data = new ObjectMapper().readValue(sig, classOf[java.util.Map[String,Double]]).asScala.toList
        val query = Series(data:_*)
        val search = model_db.map( x => {
          val common = query.index.intersect(x._2.index)
          val nquery = query.reindex(common.index).values
          val nelement = x._2.reindex(common.index).values
          if (common.index.length > overlap) {
            val corr_val = new PearsonsCorrelation().correlation(nquery.toSeq.toArray, nelement.toSeq.toArray)
            (x._1, corr_val, nelement.zipMap(common.index.toVec)( (y,z) => (z,y)).toSeq.toMap,
              Map( "size" -> x._2.length, "params" -> x._3, "coef" -> x._2.toSeq.toMap ))
          } else {
            (x._1, Double.NaN, nelement.zipMap(common.index.toVec)( (y,z) => (z,y)).toSeq.toMap,
              Map( "size" -> x._2.length ) )
          }
        } ).filter( ! _._2.isNaN ).sortBy( x => -x._2 )
        val outstream = response.getOutputStream
        val out = JSON_FACTORY.createGenerator(outstream)
        search.take(100).foreach( x => {
          val m = Map(
            "label" -> x._1,
            "score" -> x._2,
            "values" -> x._3.asJava,
            "signature" -> x._4.asJava
          ).asJava
          out.writeObject(m)
          outstream.write("\n".getBytes)
        }
        )
      }
    }

    context.addServlet(classOf[RootServlet], "")
    context.addServlet(classOf[SignatureSearch], "/search_sig")

    server.setHandler(context)
    server.start
    server.join

  }
}