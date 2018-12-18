package es.pue.mediaset.share

import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties, TimeZone}

import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Inversion {

  val utils = new Utils

  def main(args : Array[String]) {

    val cfg = new ConfigArgs(args)

    val process_month = cfg.getMesEnCurso

    val parametrization_filename = cfg.getParametrizationFileName

    val log = LogManager.getRootLogger

    val parametrizationCfg = utils.loadPropertiesFromPath(parametrization_filename)

    // Salesforce credencials
    val loginPRO = parametrizationCfg.getProperty("sf.login.pro")
    val usernamePRO = parametrizationCfg.getProperty("sf.username.pro")
    val passwordPRO = parametrizationCfg.getProperty("sf.password.pro")

    val spark = SparkSession.builder.appName("mediaset-share").getOrCreate()

    /************************************************************************************************************/
    // Salesforce

    val salesforce = new Salesforce()

    salesforce.setCredentials(parametrizationCfg, "pro")

    val tb_coeficientes: DataFrame = salesforce.get_tb_coeficientes(spark, salesforce.query_tb_coeficientes)
    val cat_gr_cadenas_n2: DataFrame = salesforce.get_cat_gr_cadenas_n2(spark, salesforce.query_cat_gr_cadenas_n2)
    val cat_gr_cadenas_n1: DataFrame = salesforce.get_cat_gr_cadenas_n1(spark, salesforce.query_cat_gr_cadenas_n1)
    val cat_gr_cadenas: DataFrame = salesforce.get_cat_gr_cadenas(spark, salesforce.query_cat_gr_cadenas)
    val cat_eventos: DataFrame = salesforce.get_cat_eventos(spark, salesforce.query_cat_eventos)
    val cat_coeficientes: DataFrame = salesforce.get_cat_coeficientes(spark, salesforce.query_cat_coeficientes)
    val cat_nuevas_cadenas: DataFrame = salesforce.get_cat_nuevas_cadenas(spark, salesforce.query_cat_nuevas_cadenas)


  }

}
