package es.pue.mediaset.share

import java.util.Properties
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object Inversion {

  // Instanciación de clase de utilidades
  val utils = new Utils

  def main(args: Array[String]) {

    val cfg = new ConfigArgs(args)

    val process_month = cfg.getMesEnCurso

    val parametrization_filename = cfg.getParametrizationFileName

    val parametrizationCfg = utils.loadPropertiesFromPath(parametrization_filename)

    // Input tables
    val input_db = parametrizationCfg.getProperty("mediaset.inv.input.db")
    val output_db = parametrizationCfg.getProperty("mediaset.inv.output.db")
    val tbl_rel_campania_trgt = parametrizationCfg.getProperty("mediaset.share.input.tbl_name_rel_campania_trgt")
    val tbl_dim_agrup_cadenas = parametrizationCfg.getProperty("mediaset.share.output.tbl_name_dim_agrup_cadenas")

    val spark = SparkSession.builder.appName("mediaset-inv").getOrCreate()
    import spark.implicits._

    /** **********************************************************************************************************/
    // Salesforce

    val salesforce = new Salesforce()

    salesforce.setCredentials(parametrizationCfg, "pro")

    val tb_coeficientes: DataFrame = salesforce.get_tb_coeficientes(spark, salesforce.query_tb_coeficientes)
    tb_coeficientes.createOrReplaceTempView("tb_coeficientes")

    val cat_eventos: DataFrame = salesforce.get_cat_eventos(spark, salesforce.query_cat_eventos)
    cat_eventos.createOrReplaceTempView("cat_eventos")
    cat_eventos.show(35)

    val cat_coeficientes: DataFrame = salesforce.get_cat_coeficientes(spark, salesforce.query_cat_coeficientes)
    cat_coeficientes.createOrReplaceTempView("cat_coeficientes")

    val cat_gr_cadenas_n2: DataFrame = salesforce.get_cat_gr_cadenas_n2(spark, salesforce.query_cat_gr_cadenas_n2)
    val cat_gr_cadenas_n1: DataFrame = salesforce.get_cat_gr_cadenas_n1(spark, salesforce.query_cat_gr_cadenas_n1)
    val cat_gr_cadenas: DataFrame = salesforce.get_cat_gr_cadenas(spark, salesforce.query_cat_gr_cadenas)
    val cat_nuevas_cadenas: DataFrame = salesforce.get_cat_nuevas_cadenas(spark, salesforce.query_cat_nuevas_cadenas)


    // Collecting data from SF
    val cat_eventos_list = spark.sql("SELECT * FROM cat_eventos").as[CatEventos].collect().toList
    val agrupCadenas_list = spark.sql(s"""SELECT * FROM $output_db.$tbl_dim_agrup_cadenas""").as[AgrupCadenas].collect().toList
    val rel_campania_trgt_list = spark.sql(s"""SELECT * FROM $input_db.$tbl_rel_campania_trgt""").as[relCampaniaTrgt].collect().toList


    // Creating broadcast objects to work on the nodes
    val BC_cat_eventos_list = spark.sparkContext.broadcast(cat_eventos_list)
    val BC_agrupCadenas_list = spark.sparkContext.broadcast(agrupCadenas_list)
    val BC_rel_campania_trgt_list = spark.sparkContext.broadcast(rel_campania_trgt_list)

    val codigos_de_cadenas_campemimediaset: List[Long] = spark.sql(
      s"""SELECT DISTINCT cod_cadena FROM $output_db.$tbl_dim_agrup_cadenas WHERE des_grupo_n1 = "MEDIASET"
      """.stripMargin).map(r => r.getInt(0).toLong).collect.toList

    val BC_codigos_de_cadenas_campemimediaset: Broadcast[List[Long]] = spark.sparkContext.broadcast(codigos_de_cadenas_campemimediaset)


    // Calculo de nuevas columnas
    // TODO Función para recuperar la tabla mediaset.fctd_share_grps persistida en Cloudera
    val fctd_share_grps: DataFrame = get_fctd_share_grps(spark, parametrizationCfg)

    val fctm_share_inv_col_1: DataFrame = getColumn_importe_pase(spark, fctd_share_grps)

    val fctm_share_inv_col_2: DataFrame = getColumn_costebase(spark, fctm_share_inv_col_1)

    val fctm_share_inv_col_3: DataFrame = getColumn_coef_evento(spark, fctm_share_inv_col_2, BC_cat_eventos_list)


    persistShareINV(fctm_share_inv_col_3, parametrizationCfg) // Persistimos en Hive el ultimo DF y se guarda como tabla en Hive

    spark.stop()
  }

  def persistShareINV(newDF: DataFrame, parametrizationCfg: Properties) {

    //    val toPartitionDF = newDF.withColumn("fecha_part", expr("substring(fecha_dia, 1, 7)")) //TODO consultar si está tabla se va a particionar o no...
    val toPartitionDF = newDF

    persistAsTableShareINV(toPartitionDF, parametrizationCfg, "fctm_share_inv")

  }

  def persistAsTableShareINV(newDF: DataFrame, parametrizationCfg: Properties, table: String) {

    val output_db = parametrizationCfg.getProperty("mediaset.inv.output.db")

    val tbl_name: String = parametrizationCfg.getProperty("mediaset.inv.output.tbl_name_" + table)

    val tbl_location: String = parametrizationCfg.getProperty("mediaset.inv.output.tbl_location_" + table)

    val output_tbl_format: String = parametrizationCfg.getProperty("mediaset.inv.output.tbl_format_" + table)

    val output_tbl_compression: String = parametrizationCfg.getProperty("mediaset.inv.output.tbl_compression_" + table)

    newDF.write.mode("overwrite").format(output_tbl_format).option("compression", output_tbl_compression).option("path", tbl_location).saveAsTable(s"$output_db.$tbl_name")

  }

  // TODO UDF's importe_pase  ----------------------------------------------------------------------------

  def getColumn_importe_pase(spark: SparkSession, originDF: DataFrame): DataFrame = {

    originDF.withColumn("importe_pase", lit(40.000))
  }

  // TODO UDF's costebase  ----------------------------------------------------------------------------

  def getColumn_costebase(spark: SparkSession, originDF: DataFrame): DataFrame = {

    originDF.withColumn("costebase", lit(1.000))
  }

  // UDF's coef_evento  ----------------------------------------------------------------------------

  def getColumn_coef_evento(spark: SparkSession, originDF: DataFrame, BC_cat_eventos_list: Broadcast[List[CatEventos]]): DataFrame = {

    originDF.withColumn("coef_evento", UDF_coef_evento(BC_cat_eventos_list)(col("cod_eventos")))

  }

  def UDF_coef_evento(BC_cat_eventos_list: Broadcast[List[CatEventos]]): UserDefinedFunction = {

    udf[Double, Long](cod_eventos => FN_coef_evento(BC_cat_eventos_list.value, cod_eventos))
  }

  def FN_coef_evento(catEventosList: List[CatEventos], cod_eventos: Long): Double = {

    var result = 0D

    for (elem <- catEventosList) {
      if (cod_eventos == 1) {
        result = elem.indice.toDouble
      } else {
        result = 1D
      }
    }
    result
  }

  // UDF's POR_TP_MEDIASET  ----------------------------------------------------------------------------

  def getColumn_por_tp_mediaset(spark: SparkSession, originDF: DataFrame, BC_agrupCadenas_list: Broadcast[List[AgrupCadenas]]): DataFrame = {

    originDF.withColumn("por_tp_mediaset", UDF_por_tp_mediaset(BC_agrupCadenas_list)(col("des_day_part")))
  }

  def UDF_por_tp_mediaset(BC_agrupCadenas_list: Broadcast[List[AgrupCadenas]]): UserDefinedFunction = {

    udf[Double, String](des_day_part => FN_por_tp_mediaset(BC_agrupCadenas_list.value, des_day_part))
  }

  def FN_por_tp_mediaset(agrupCadenas_list: List[AgrupCadenas], des_day_part: String): Double = {
    // TODO
    2D
  }



  // Case class con las tablas de SalesForce

  case class LineaNegocio(cod_comunicacion: java.lang.Long, cod_tipologia: java.lang.Long, cod_tp_categr_km: java.lang.Long, cod_tp_computo_km: java.lang.Long,
                          cod_tp_lineanegocio_km: java.lang.Long, des_comunicacion: String, des_tipologia: String,
                          fecha_fin: Long,fecha_ini: Long, nom_tp_categr_km: String, nom_tp_computo_km: String, nom_tp_lineanegocio_km: String )

  case class AgrupCadenas(des_grupo_n1 : String, des_grupo_n2: String, des_grupo_n0: String, fecha_fin: Long, cod_forta: java.lang.Long, des_forta: String,
                          cod_cadena: java.lang.Long, cod_grupo_n0: java.lang.Long, fecha_ini: Long,
                          cod_grupo_n2: java.lang.Long, cod_grupo_n1: java.lang.Long, des_cadena: String)

  case class relCampaniaTrgt(cod_anuncio: java.lang.Long, nom_anuncio: String, cod_cadena: java.lang.Long, nom_cadena: String, cod_target: java.lang.Long, cod_target_gen_may: java.lang.Long,
                             nom_target_gen_may: String, fecha_hora: java.lang.Long, origen_datos: String)


  case class Configuraciones(cod_accion : java.lang.Long, cod_anunciante_kantar: java.lang.Long, cod_anunciante_pe: java.lang.Long, cod_cadena: java.lang.Long,
                             cod_campana: java.lang.Long, cod_programa: java.lang.Long, cod_tipologia: java.lang.Long, des_accion: String, des_anunciante_kantar: String,
                             des_anunciante_pe: String, des_cadena: String, des_campana: String, des_programa: String, des_tipologia: String, fecha_fin: Long, fecha_ini: Long, iiee2_formato: String)

  case class Parametros(des_parametro : String, fecha_fin: Long, fecha_ini: Long, nom_param: String, valor: String)

  case class Coeficientes(Anyo : String, coeficiente: String, des_cadena: String, fecha_act: Long, fecha_fin: Long, fecha_ini: Long, flag: java.lang.Long, INDICE: String, MAX_RANGO: String,
                          Mes: String, MIN_RANGO: String)

  case class Eventos(cod_cadena : java.lang.Long, cod_evento: java.lang.Long, cod_programa: java.lang.Long, des_cadena: String, des_evento: String,
                     des_programa: String, fecha_fin: Long, fecha_ini: Long, flag: java.lang.Long)

  case class Cadenas_n2(cod_grupo_n2: java.lang.Long, des_grupo_n2: String, fecha_fin: Long, fecha_ini: Long)

  case class Cadenas_n1(cod_grupo_n1: java.lang.Long, des_grupo_n1: String, fecha_fin: Long, fecha_ini: Long)

  case class Cadenas(cod_grupo_n0: java.lang.Long, des_grupo_n0: String, fecha_fin: Long, fecha_ini: Long)

  case class CatEventos(cod_evento: java.lang.Long, activo: String, des_evento: String, fecha_fin: Long, fecha_ini: Long, indice: String)

  case class CatCoeficientes(cod_coef: java.lang.Long, des_coef: String, fecha_fin: Long, fecha_ini: Long)

  case class CatNuevasCadenas(cod_cadena_nueva: java.lang.Long, des_cadena_n: String, fecha_fin: Long, fecha_ini: Long)


  def get_fctd_share_grps(spark: SparkSession, parametrizationCfg: Properties): DataFrame = {

    val input_db: String = parametrizationCfg.getProperty("mediaset.share.input.db")
    val tbl_name_fctd_share_grps: String = parametrizationCfg.getProperty("mediaset.inv.input.tbl_name_fctd_share_grps")

    spark.sql(s"""SELECT * FROM $input_db.$tbl_name_fctd_share_grps""")

  }

}
