package edu.ucsc.sig_server


class SignatureHit(
                    var signature : Signature = null,
                    var score : Double = 0.0,
                    var overlap : Int= 0) extends Serializable {



}
