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

  var model_db : RDD[(String, Series[String,Double])] = null

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
      (x.get("label").asInstanceOf[String], Series( coef :_* ))
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
        val data = new ObjectMapper().readValue(sig, classOf[java.util.Map[String,Double]]).asScala.toList
        val query = Series(data:_*)
        val search = model_db.map( x => {
          val common = query.index.intersect(x._2.index)
          val dot = query.reindex(common.index).values.dot( x._2.reindex(common.index).values )
          (x._1, dot)
        } ).sortBy( x => x._2 )
        response.getWriter().println(search.take(100).mkString(","))
      }
    }

    context.addServlet(classOf[RootServlet], "")
    context.addServlet(classOf[SignatureSearch], "/search_sig")

    server.setHandler(context)
    server.start
    server.join

  }
}