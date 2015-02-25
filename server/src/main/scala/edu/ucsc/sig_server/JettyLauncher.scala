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

  var model_db : RDD[Signature] = null

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

      new Signature(
        label=x.get("label").asInstanceOf[String],
        method=x.get("method").asInstanceOf[String],
        coef=Series( coef :_* ),
        params=x.get("params").asInstanceOf[java.util.Map[String,Any]].asScala.toMap
      )
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
        val request_text = request.getReader.readLine()
        val request_data = new ObjectMapper().readValue(request_text, classOf[java.util.Map[String,Any]]).asScala

        val overlap = request_data.getOrElse("overlap", "1").toString.toInt
        val limit = request_data.getOrElse("limit", "100").toString.toInt
        val weights = request_data("weights").asInstanceOf[java.util.Map[String,Double]].asScala
        val query = Series(weights.toSeq:_*)

        val search = model_db.map( x => {
          val common = query.index.intersect(x.coef.index)
          val nquery = query.reindex(common.index).values
          val nelement = x.coef.reindex(common.index).values
          if (common.index.length > overlap) {
            val corr_val = new PearsonsCorrelation().correlation(nquery.toSeq.toArray, nelement.toSeq.toArray)
            new SignatureHit( x, corr_val, common.index.length )
          } else {
            null.asInstanceOf[SignatureHit]
          }
        } ).filter( _ != null ).sortBy( x => -x.score )
        val outstream = response.getOutputStream
        val out = JSON_FACTORY.createGenerator(outstream)
        out.writeStartObject()
        out.writeObjectField("query", request_data.asJava)
        out.writeObjectFieldStart("signatures")
        search.take(limit).foreach( x => {
          out.writeObjectFieldStart(x.signature.label)
          out.writeNumberField("score", x.score)
          out.writeObjectFieldStart("weights")
          x.signature.coef.toSeq.foreach( y => out.writeObjectField(y._1, y._2) )
          out.writeEndObject()
          out.writeEndObject()
        })
        out.writeEndObject()
        out.writeEndObject()
        out.flush()
        outstream.flush()
      }
    }

    context.addServlet(classOf[RootServlet], "")
    context.addServlet(classOf[SignatureSearch], "/search_sig")

    server.setHandler(context)
    server.start
    server.join

  }
}