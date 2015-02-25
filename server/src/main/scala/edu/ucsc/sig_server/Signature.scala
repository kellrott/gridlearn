package edu.ucsc.sig_server

import org.saddle.Series

class Signature(
                 var label : String = null,
                 var method : String = null,
                 var coef : Series[String,Double] = null,
                 var params : Map[String,Any] = null) extends Serializable {


}
