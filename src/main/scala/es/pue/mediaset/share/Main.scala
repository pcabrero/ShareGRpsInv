package es.pue.mediaset.share

import org.apache.spark.sql.SparkSession

object Main {

  def main(args : Array[String]) {

    val cfg = new ConfigArgs(args)

    val spark = SparkSession.builder.appName("mediaset-share").getOrCreate()

    cfg.getProcess match {

      case "Share" =>       Share.main(args)
      case "Inversion" =>   Inversion.main(args)
      case  default => processNotFound(cfg.getProcess)

    }

  }

  def processNotFound(process : String) : Unit = {

    println(s"No se ha encontrado proceso para el parámetro : $process")
    sys.exit(1)

  }


}
