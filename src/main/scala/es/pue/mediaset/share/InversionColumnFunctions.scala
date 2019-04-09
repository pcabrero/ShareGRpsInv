package es.pue.mediaset.share

import es.pue.mediaset.share.Inversion.Coeficientes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf, when}
import org.apache.spark.sql.types.DoubleType

import scala.collection.immutable.Map

object InversionColumnFunctions {

  def getColumn_inv_est_pe(originDF: DataFrame, selectDF: DataFrame): DataFrame = {
    originDF.join(selectDF,Seq("gr_n1_infoadex"),"left_outer")
  }

  /************************************************************************************************************/

  def getColumn_coef_corrector_inv_agregada(originDF: DataFrame): DataFrame = {

    originDF.withColumn("coef_corrector", UDF_coef_corrector_inv_agregada()(col("inv_est_infoadex"), col("inv_est_pe")))

  }

  def UDF_coef_corrector_inv_agregada(): UserDefinedFunction = {
    udf[java.lang.Double, java.lang.Double, java.lang.Double] ( (inv_est_infoadex, inv_est_pe) => FN_coef_corrector_inv_agregada(inv_est_infoadex, inv_est_pe))
  }

  def FN_coef_corrector_inv_agregada(inv_est_infoadex: java.lang.Double, inv_est_pe: java.lang.Double): java.lang.Double = {

    try {
      Some(((inv_est_infoadex - inv_est_pe) / inv_est_pe) + 1).get
    } catch {
      case e: Exception => 0D
    }
  }



  /************************************************************************************************************/

  def getColumn_cod_fg_cadmediaset(originDF: DataFrame, BC_cadenas_mediaset_grupo_n1: Broadcast[Set[Long]]): DataFrame = {
    originDF.withColumn("cod_fg_cadmediaset", UDF_cod_fg_cadmediaset(BC_cadenas_mediaset_grupo_n1)(col("cod_cadena")))
  }

  def UDF_cod_fg_cadmediaset(BC_cadenas_mediaset_grupo_n1: Broadcast[Set[Long]]): UserDefinedFunction = {

    udf[Int, java.lang.Long]( cod_cadena => FN_cod_fg_cadmediaset(BC_cadenas_mediaset_grupo_n1.value, cod_cadena))

  }

  def FN_cod_fg_cadmediaset(cadenasMediasetGrupo_n1: Set[Long], cod_cadena: java.lang.Long): Int =  {

    var result = 0
    if( cadenasMediasetGrupo_n1.contains(cod_cadena)) {
      result = 1
    }
    result
  }

  /************************************************************************************************************/

  def getColumn_cod_fg_prodmediaset(originDF: DataFrame, BC_producto_mediaset: List[Long]): DataFrame = {
    originDF.withColumn("cod_fg_prodmediaset", UDF_cod_fg_prodmediaset(BC_producto_mediaset)(col("cod_producto")))
  }

  def UDF_cod_fg_prodmediaset(BC_producto_mediaset: List[Long]): UserDefinedFunction = {

    udf[Int, java.lang.Long]( cod_producto => FN_cod_fg_prodmediaset(BC_producto_mediaset, cod_producto))
  }

  def FN_cod_fg_prodmediaset(productoMediaset_list: List[Long], cod_producto: java.lang.Long): Int = {

    var result = 0
    if ( productoMediaset_list.contains(cod_producto)) {
      result = 1
    }
    result
  }

  /************************************************************************************************************/

  def getColumn_cod_fg_grupomediaset(originDF: DataFrame, BC_grupo_mediaset: List[Long]): DataFrame = {
    originDF.withColumn("cod_fg_grupomediaset", UDF_cod_fg_grupomediaset(BC_grupo_mediaset)(col("cod_grupo")))
  }

  def UDF_cod_fg_grupomediaset(BC_grupo_mediaset: List[Long]): UserDefinedFunction = {
    udf[Int, java.lang.Long](cod_grupo => FN_cod_fg_grupomediaset(BC_grupo_mediaset, cod_grupo))
  }

  def FN_cod_fg_grupomediaset(grupoMediaset_list: List[Long], cod_grupo: java.lang.Long): Int = {

    var result = 0
    if( grupoMediaset_list.contains(cod_grupo)) {
      result = 1
    }
    result
  }

  /************************************************************************************************************/

  def getColumn_importe_pase(originDF: DataFrame,selectDF: DataFrame): DataFrame = {
    originDF.join(selectDF,Seq("aniomes","cod_cadena","cod_target_compra","cod_anunc","cod_anunciante_subsidiario","cod_anuncio","cod_marca"),"left_outer")
  }

  /************************************************************************************************************/

  def getColumn_costebase(spark: SparkSession, originDF: DataFrame, costeBase_marca: DataFrame, costeBase_anunc: DataFrame, costeBase_producto: DataFrame, costeBase_grupo: DataFrame, costeBase_else: DataFrame): DataFrame = {

    import spark.implicits._

    val costeBaseDf_1 = originDF.join(costeBase_marca, Seq("aniomes","cod_anuncio"), "left_outer")
    val costeBaseDf_2 = costeBaseDf_1.join(costeBase_anunc, Seq("aniomes","cod_anunc"), "left_outer")
    val costeBaseDf_3 = costeBaseDf_2.join(costeBase_producto, Seq("aniomes","cod_producto"), "left_outer")
    val costeBaseDf_4 = costeBaseDf_3.join(costeBase_grupo, Seq("aniomes","cod_grupo"), "left_outer")
    val costeBaseDf_5 = costeBaseDf_4.join(costeBase_else, Seq("aniomes","cod_anuncio"), "left_outer")

    val finalDf = costeBaseDf_5.withColumn("costeBase",
      when($"cod_fg_cadmediaset" === 0 and $"cod_fg_campemimediaset" === 1, $"costebase_marca")
    .when($"cod_fg_cadmediaset" === 0 and $"cod_fg_campemimediaset" === 0 and $"cod_fg_anuncmediaset" === 1, $"costebase_anunc")
    .when($"cod_fg_cadmediaset" === 0 and $"cod_fg_campemimediaset" === 1 and $"cod_fg_anuncmediaset" === 0 and $"cod_fg_prodmediaset" === 1, $"costebase_producto")
    .when($"cod_fg_cadmediaset" === 0 and $"cod_fg_campemimediaset" === 1 and $"cod_fg_anuncmediaset" === 0 and $"cod_fg_prodmediaset" === 0 and $"cod_fg_grupomediaset" === 1, $"costebase_grupo")
    .when($"cod_fg_cadmediaset" === 1, $"costebase_else").otherwise(0D))
      .drop($"costebase_marca")
      .drop($"costebase_anunc")
      .drop($"costebase_producto")
      .drop($"costebase_grupo")
      .drop($"costebase_else")

    finalDf

  }



  /************************************************************************************************************/

  def getColumn_coef_evento(spark: SparkSession, originDF: DataFrame,selectDF: DataFrame): DataFrame = {

    import spark.implicits._
    originDF.join(selectDF,Seq("cod_eventos"),"left_outer").withColumn("Coef_evento", when($"Coef_evento".isNull, 1L ).otherwise($"Coef_evento"))

  }


  /************************************************************************************************************/

  def getColumn_por_pt_mediaset(spark: SparkSession, originDF: DataFrame,selectDF: DataFrame): DataFrame = {

    import spark.implicits._
    originDF.join(selectDF,Seq("aniomes","cod_anuncio","cod_anunc","cod_cadena"),"left_outer").withColumn("por_pt_mediaset", when($"por_pt_mediaset".isNull, 1D ).otherwise($"por_pt_mediaset"))

  }


  /************************************************************************************************************/

  def getColumn_por_pt_grupocadena(spark: SparkSession,  originDF: DataFrame,selectDF: DataFrame): DataFrame = {

    import spark.implicits._

    originDF.join(selectDF,Seq("aniomes","cod_cadena","cod_anuncio","fecha_dia"),"left_outer").withColumn("por_pt_grupocadena", when($"por_pt_grupocadena".isNull, 1D ).otherwise($"por_pt_grupocadena"))
  }


  /************************************************************************************************************/

  def getColumn_dif_por_primetime( originDF: DataFrame) : DataFrame = {
    originDF.withColumn("dif_por_primetime", col("por_pt_grupocadena") - col("por_pt_mediaset"))
  }

  /************************************************************************************************************/

  def getColumn_coef_pt( originDF: DataFrame, BC_tb_coeficientes_list: List[Coeficientes],BC_cod_cadena_disney: List[Long]): DataFrame = {
    originDF.withColumn("coef_pt", UDF_coef_pt(BC_tb_coeficientes_list,BC_cod_cadena_disney)(col("dif_por_primetime"),col("aniomes"),col("cod_cadena")))
  }

  def UDF_coef_pt(BC_tb_coeficientes_list: List[Coeficientes],BC_cod_cadena_disney_list:List[Long]): UserDefinedFunction = {

    udf[Double, java.lang.Double, java.lang.Long, java.lang.Long]( (dif_por_primetime,aniomes,cod_cadena)  => FN_coef_pt( BC_tb_coeficientes_list,BC_cod_cadena_disney_list, dif_por_primetime,aniomes,cod_cadena) )

  }

  def FN_coef_pt(Coeficientes_list: List[Coeficientes], cod_cadena_disney: List[Long], dif_por_primetime: java.lang.Double, aniomes : java.lang.Long, cod_cadena : java.lang.Long): Double = {

    var result = 1D
    if(!cod_cadena_disney.contains(cod_cadena)){
      for (elem <- Coeficientes_list) {
        if ( elem.coeficiente.equalsIgnoreCase("PRIMETIME")
          && dif_por_primetime > elem.MIN_RANGO
          && dif_por_primetime <= elem.MAX_RANGO
          && aniomes >= UtilsInversion.transformToAniomes(elem.fecha_ini)
          && aniomes <= UtilsInversion.transformToAniomes(elem.fecha_fin)){
          result = elem.INDICE
        }
      }
    }
    result
  }

  /************************************************************************************************************/

  def getColumn_por_cualmediaset(spark: SparkSession, originDF: DataFrame, selectDF: DataFrame): DataFrame = {

    import spark.implicits._
    originDF.join(selectDF,Seq("cod_cadena","fecha_dia"),"left_outer").withColumn("por_cual_mediaset", when($"por_cual_mediaset".isNull, 1D ).otherwise($"por_cual_mediaset"))
  }


  /************************************************************************************************************/

  def getColumn_por_cualgrupocadena(spark: SparkSession, originDF: DataFrame, selectDF: DataFrame): DataFrame = {

    import spark.implicits._
    originDF.join(selectDF,Seq("cod_cadena", "aniomes","fecha_dia"),"left_outer").withColumn("por_cualgrupocadena", when($"por_cualgrupocadena".isNull, 1D ).otherwise($"por_cualgrupocadena"))
  }


  /************************************************************************************************************/

  def getColumn_dif_por_cualitativos( originDF: DataFrame ) : DataFrame = {

    originDF.withColumn("dif_por_cualitativos", col("por_cualgrupocadena") - col("por_cual_mediaset"))
  }

  /************************************************************************************************************/

  def getColumn_coef_cual( originDF: DataFrame, BC_tb_coeficientes_list: List[Coeficientes]) : DataFrame = {

    originDF.withColumn("coef_cual", UDF_coef_cual(BC_tb_coeficientes_list)(col("dif_por_cualitativos"),col("aniomes")))

  }

  def UDF_coef_cual(BC_tb_coeficientes_list: List[Coeficientes]): UserDefinedFunction = {

    udf[Double, java.lang.Double, java.lang.Long]( (dif_por_cualitativos,aniomes)  => FN_coef_cual( BC_tb_coeficientes_list, dif_por_cualitativos,aniomes) )
  }

  def FN_coef_cual( Coeficientes_list: List[Coeficientes], dif_por_cualitativos: java.lang.Double, aniomes: java.lang.Long ): Double = {

    var result = 1D
    for (elem <- Coeficientes_list) {
      if ( elem.coeficiente.equalsIgnoreCase("Cualitativos")
        && dif_por_cualitativos > elem.MIN_RANGO
        && dif_por_cualitativos <= elem.MAX_RANGO
        && aniomes >= UtilsInversion.transformToAniomes(elem.fecha_ini)
        && aniomes <= UtilsInversion.transformToAniomes(elem.fecha_fin)) {
        result = elem.INDICE
      }
    }
    result
  }

  /************************************************************************************************************/

  def getColumn_cod_posicionado( originDF: DataFrame): DataFrame = {

    originDF.withColumn("cod_posicionado", UDF_cod_posicionado()(col("cod_cualitativo"), col("cod_posicion_pb2")))
  }

  def UDF_cod_posicionado() : UserDefinedFunction = {
    udf[Long, java.lang.Long, java.lang.Long]( (cod_cualitativo, cod_posicion_pb2) => FN_cod_posicionado(cod_cualitativo, cod_posicion_pb2))
  }

  def FN_cod_posicionado(cod_cualitativo: java.lang.Long, cod_posicion_pb2: java.lang.Long): Long = {

    var result = 0L

    val posicionados : Set[Long] = Set(1,2,3,997,998,999)

    if(cod_cualitativo != 0 && posicionados.contains(cod_posicion_pb2)){
      result = 1L
    }

    result
  }

  /************************************************************************************************************/

  def getColumn_por_posmediaset(spark: SparkSession, originDF: DataFrame, selectDF: DataFrame): DataFrame = {

    import spark.implicits._

    originDF.join(selectDF,Seq("cod_cadena","fecha_dia"), "left_outer").withColumn("por_posmediaset", when($"por_posmediaset".isNull, 1D ).otherwise($"por_posmediaset"))

  }

  /************************************************************************************************************/

  def getColumn_por_posGrupoCadena_n1(spark: SparkSession, originDF: DataFrame, selectDF: DataFrame): DataFrame = {

    import spark.implicits._

    originDF.join(selectDF,Seq("cod_cadena","fecha_dia"),"left_outer").withColumn("por_posGrupoCadenaN1", when($"por_posGrupoCadenaN1".isNull, 1D ).otherwise($"por_posGrupoCadenaN1"))

  }

  /************************************************************************************************************/

  def getColumn_dif_por_posicionamiento( originDF: DataFrame): DataFrame = {


    originDF.withColumn("dif_por_posicionamiento", col("por_posGrupoCadenaN1") - col("por_posmediaset"))
  }


  /************************************************************************************************************/

  def getColumn_coef_posic( originDF: DataFrame,  BC_tb_coeficientes_list: List[Coeficientes]): DataFrame = {

    originDF.withColumn("coef_posic", UDF_coef_posic(BC_tb_coeficientes_list)(col("dif_por_posicionamiento"), col("cod_cualitativo"),col("aniomes")))

  }

  def UDF_coef_posic(BC_tb_coeficientes_list: List[Coeficientes]): UserDefinedFunction = {

    udf[Double, java.lang.Double, java.lang.Long, java.lang.Long]( (dif_por_posicionamiento,cod_fg_cualitativo,aniomes)  => FN_coef_posic( BC_tb_coeficientes_list, dif_por_posicionamiento,cod_fg_cualitativo,aniomes) )

  }

  def FN_coef_posic(Coeficientes_list: List[Coeficientes], dif_por_posicionamiento: java.lang.Double, cod_fg_cualitativo: java.lang.Long, aniomes: java.lang.Long): Double = {

    var result = 1D
    if (cod_fg_cualitativo != 1) {
      for (elem <- Coeficientes_list) {
        if (elem.coeficiente.equalsIgnoreCase("POSICIONAMIENTO")
          && dif_por_posicionamiento > elem.MIN_RANGO
          && dif_por_posicionamiento <= elem.MAX_RANGO
          && aniomes >= UtilsInversion.transformToAniomes(elem.fecha_ini)
          && aniomes <= UtilsInversion.transformToAniomes(elem.fecha_fin)) {
          result = elem.INDICE
        }
      }
    }
    result
  }

  /************************************************************************************************************/

  def getColumn_cuota_por_grupo( spark: SparkSession, originDF: DataFrame, selectDF: DataFrame): DataFrame = {

    import spark.implicits._
    //originDF.withColumn("cuota_por_grupo", UDF_cuota_por_grupo(BC_cuota_por_grupo_calc)(col("aniomes"),col("cod_grupo_n1"),col("fecha_dia"),col("fecha_ini"),col("fecha_fin")))
    originDF.join(selectDF,Seq("aniomes","cod_cadena","fecha_dia"), "left_outer")
      .withColumn("cuota_por_grupo", when($"cod_fg_cadmediaset" === 0 , $"cuota").otherwise(1D))
      .withColumn("cuota_por_grupo", when($"cuota_por_grupo".isNull , 0D).otherwise($"cuota_por_grupo"))
      .drop("cuota")

  }

  /************************************************************************************************************/

  def getColumn_coef_cuota( originDF: DataFrame, BC_tb_coeficientes_list: List[Coeficientes]): DataFrame = {

    originDF.withColumn("coef_cuota", UDF_coef_cuota(BC_tb_coeficientes_list)(col("cuota_por_grupo"),col("aniomes")))

  }

  def UDF_coef_cuota(BC_tb_coeficientes_list: List[Coeficientes]): UserDefinedFunction = {

    udf[Double, java.lang.Double, java.lang.Long]( (cuota_por_grupo,aniomes)  => FN_coef_cuota( BC_tb_coeficientes_list, cuota_por_grupo, aniomes) )

  }

  def FN_coef_cuota(Coeficientes_list: List[Coeficientes], cuota_por_grupo: java.lang.Double, aniomes: java.lang.Long): Double = {

    var result = 1D
    for (elem <- Coeficientes_list) {
      if ( elem.coeficiente.equalsIgnoreCase("CUOTA")
        && cuota_por_grupo > elem.MIN_RANGO
        && cuota_por_grupo <= elem.MAX_RANGO
        && aniomes >= UtilsInversion.transformToAniomes(elem.fecha_ini)
        && aniomes <= UtilsInversion.transformToAniomes(elem.fecha_fin)) {
        result = elem.INDICE
      }
    }
    result
  }

  /************************************************************************************************************/


  def getColumn_coef_cadena( spark: SparkSession, originDF: DataFrame,  coef_cadena_cacl1: DataFrame, coef_cadena_cacl2: DataFrame): DataFrame = {

    //originDF.withColumn("coef_cadena", UDF_coef_cadena(BC_coef_cadena_cacl1,BC_coef_cadena_cacl2)(col("cod_cadena"),col("cod_fg_forta")))

    import spark.implicits._
    val coef_cadena_df1 = originDF.join(coef_cadena_cacl1, Seq("cod_cadena"),"left_outer")
    val coef_cadena_df2 = coef_cadena_df1.join(coef_cadena_cacl2,Seq("cod_cadena"),"left_outer")
    val result = coef_cadena_df2.withColumn("coef_cadena", when($"cod_cadena" === 30006 and $"cod_fg_forta" === 0, $"coef_cadena1")
      .when($"cod_cadena" !== 30006, $"coef_cadena2").otherwise(1D))
      .withColumn("coef_cadena", when($"coef_cadena".isNull, 1D).otherwise($"coef_cadena"))
        .drop("coef_cadena1")
        .drop("coef_cadena2")
    result
  }

  /*
  def UDF_coef_cadena(BC_coef_cadena_cacl: Broadcast[Map[Long, Double]], BC_coef_cadena_cacl2: Broadcast[Map[Long, Double]]): UserDefinedFunction = {

    udf[Double, java.lang.Long, java.lang.Long]( (cod_cadena,cod_fg_forta) => FN_coef_cadena(BC_coef_cadena_cacl.value, BC_coef_cadena_cacl2.value, cod_cadena, cod_fg_forta))
  }

  def FN_coef_cadena(coefCadena_cacl1: Map[Long, Double], coefCadena_cacl2: Map[Long, Double], cod_cadena: java.lang.Long, cod_fg_forta: java.lang.Long): Double = {

    var result = 1D
    if (cod_cadena == 30006 && cod_fg_forta == 0 ) {
      result = coefCadena_cacl1.getOrElse(cod_cadena, 1D)
    }
    else if(cod_cadena != 30006) {
      result = coefCadena_cacl2.getOrElse(cod_cadena, 1D)
    }

    result
  }
  */

  /************************************************************************************************************/
  /**
    * Añade la columna "num_cadenas_forta_emitido" al DataFrame de origen
    * @param BC_num_cadenas_forta_calc: Lista que contiene el numero de cadenas forta en las que ha sido emitido un determinado anuncio
    * @return
    */
  def getColumn_num_cadenas_forta_emitido( originDF: DataFrame, selectDF: DataFrame): DataFrame = {

    originDF.join(selectDF,Seq("cod_anuncio","aniomes"),"left_outer")
  }


  /************************************************************************************************************/

  def getColumn_coef_forta( originDF: DataFrame, BC_coef_forta_param_valor: Broadcast[Int], BC_indice_coeficiente_forta: Broadcast[Double]): DataFrame = {
    originDF.withColumn("coef_forta", UDF_coef_forta(BC_coef_forta_param_valor, BC_indice_coeficiente_forta)(col("cod_fg_autonomica"), col("cod_fg_forta"), col("num_cadenas_forta_emitido")))
  }

  def UDF_coef_forta(BC_coef_forta_param_valor: Broadcast[Int], BC_indice_coeficiente_forta: Broadcast[Double]): UserDefinedFunction = {
    udf[Double, Int, Int, java.lang.Long]( (cod_fg_autonomica, cod_fg_forta, num_cadenas_forta_emitido) => FN_coef_forta(BC_coef_forta_param_valor.value, BC_indice_coeficiente_forta.value, cod_fg_autonomica, cod_fg_forta, num_cadenas_forta_emitido))
  }

  def FN_coef_forta(coefFortaParam_valor: Int, indiceCoeficiente_forta: Double, cod_fg_autonomica: Int, cod_fg_forta: Int, num_cadenas_forta_emitido: java.lang.Long): Double = {

    var result = 1D
    if (cod_fg_autonomica == 1
      && cod_fg_forta == 1
      && num_cadenas_forta_emitido >= coefFortaParam_valor ) {
      result = indiceCoeficiente_forta
    }
    result

  }




  /************************************************************************************************************/

  def getColumn_coef_anunciante(spark: SparkSession, originDF: DataFrame, coef_anunciante_calc: DataFrame) : DataFrame = {

    import spark.implicits._
    originDF.alias("a").join(coef_anunciante_calc.alias("b"), originDF("cod_anunc") === coef_anunciante_calc("cod_cadena") || originDF("cod_anunciante_subsidiario") === coef_anunciante_calc("cod_cadena"), "left_outer")
      .withColumn("coef_anunciante", when($"coef_anunciante".isNull, 1D).otherwise($"coef_anunciante"))
      .drop("cod_cadena")
    //originDF.withColumn("coef_anunciante", UDF_coef_anunciante(BC_coef_anunciante_calc)(col("cod_anunc"), col("cod_anunciante_subsidiario")))
  }

  /*
  def UDF_coef_anunciante(BC_coef_anunciante_calc: Broadcast[Map[Long, Double]]): UserDefinedFunction = {
    udf[Double, java.lang.Long, java.lang.Long]( (cod_anunc, cod_anunciante_subsidiario) => FN_coef_anunciante(BC_coef_anunciante_calc.value, cod_anunc, cod_anunciante_subsidiario))
  }

  def FN_coef_anunciante(coefAnunciante_calc: Map[Long, Double], cod_anunc: java.lang.Long, cod_anunciante_subsidiario: java.lang.Long): Double = {

    var result = 1D
    result = coefAnunciante_calc.getOrElse(cod_anunc, 1D)
    if(result == 1D){
      result = coefAnunciante_calc.getOrElse(cod_anunciante_subsidiario, 1D)
    }
    result

  }
  */

  /************************************************************************************************************/

  def getColumn_inv_pre(spark: SparkSession, originDF: DataFrame) : DataFrame = {
    import spark.implicits._

    originDF.withColumn("inv_pre", when($"cod_fg_cadmediaset" === 0,
      ($"costebase" / $"grps_20_totales") * $"coef_evento" * $"coef_pt" * $"coef_cual" * $"coef_posic" * $"coef_cuota" * $"coef_cadena" * $"coef_forta" * $"coef_anunciante")
      .otherwise($"costebase" / $"grps_20_totales"))
      .withColumn("inv_pre", when($"inv_pre".isNull, 0D).otherwise($"inv_pre"))



  }

  /************************************************************************************************************/

  // UDF's INV_FINAL ----------------------------------------------------------------------------------------------

  def getColumn_inv_final( originDF: DataFrame): DataFrame = {
    originDF.withColumn("inv_final", UDF_inv_final()(col("coef_corrector"), col("inv_pre")))
  }

  def UDF_inv_final(): UserDefinedFunction = {
    udf[Double, Double, Double]( (coef_corrector, inv_pre) => FN_inv_final(coef_corrector, inv_pre))
  }

  def FN_inv_final(coef_corrector: Double, inv_pre: Double): Double = {

    try {
      Some(coef_corrector * inv_pre).get
    } catch {
      case e: Exception => 0D
    }

  }


}
