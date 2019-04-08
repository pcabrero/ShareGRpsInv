package es.pue.mediaset.share

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, from_utc_timestamp, udf}
import org.joda.time.DateTime

object UtilsInversion {


  /**
    * Añade una nueva columna a un DataFrame que almacena la última fecha de ejecución del proceso en función de una
    * zona horaria especificada mediante el fichero .properties por defecto, con objeto de que pueda ser modificada
    * @param originDF: DataFrame de origen al que se le añade la columna
    * @param timezone: Zona horaria
    * @return
    */
  def setCurrentTimeStamp(originDF: DataFrame, timezone: String): DataFrame = {
    originDF.withColumn("fecha_carga", from_utc_timestamp(current_timestamp(), timezone))
  }
  /**
    * Se registra un DataFrame como una tabla temporal especificandole un nombre
    * @param newDF: DataFrame a registrar como tabla temporal
    * @param tmpTableName: Nombre de la tabla temporal
    */
  def registerTemporalTable(newDF: DataFrame, tmpTableName: String): Unit =  {
    newDF.createOrReplaceTempView(tmpTableName)
  }

  /**
    * Guarda un dataframe como tabla con los parametros especificados mediante un fichero .properties
    * @param newDF: DataFrame a persisir como tabla
    * @param table: Nombre de la tabla de salida
    */
  def persistAsTableShareINV(newDF: DataFrame, table: Entity): Unit = {

    newDF.write.partitionBy("fecha_part").mode("overwrite").format(table.getFormat).option("compression", table.getCompression).option("path", table.getLocation).saveAsTable(table.getDBTable)
  }
  /**
    * Query inicial que recupera todas las columnas de la tabla fctd_share_grps y la columna importe_pase de la tabla ordenado
    * @param spark: Instanciación del objeto spark para poder acceder a sus métodos de SQL
    * @return: Devuelve el primer DataFrame a partir del cual se iran agregando más columnas
    */
  def getSharedGrps(spark: SparkSession, inversionSQLstrs: InversionSQLstrs): DataFrame = {
    spark.sql(inversionSQLstrs.getSharedGrps_sqlString.stripMargin)

  }

  def transformToAniomes(millis: Long): Int = {
    val year = new DateTime(millis).getYear.toString
    val month = new DateTime(millis).getMonthOfYear.toString
    year.concat(month).toInt
  }

  /**
    * Query que selecciona diferentes campos de la tabla temporal con los datos de fctd_share_grps y la columna importe_pase y además se
    * realizan unos filtros para acotar los datos
    * @param spark: Instanciación del objeto spark para poder acceder a sus métodos de SQL
    * @return
    */
  /**
    * Función que añade una nueva columna a un DataFrame en función del nombre de la columna que se le
    * especifique y el nombre que tendrá la nueva columna que se va a crear
    * @param originDF: DataFrame de origen al que se le añade la columna
    * @param lookupColName: Columna en la que se fija
    * @param newColumn: Nombre de la nueva columna
    * @return
    */
  def setNomOnColumn(originDF : DataFrame, lookupColName : String, newColumn : String): DataFrame ={
    originDF.withColumn(newColumn, UDF_set_nom_on_column()(col(lookupColName)) )
  }

  /**
    * Función que devuelve un valor de tipo String y que es aplicado a cada registro de la columna
    * @return
    */
  def UDF_set_nom_on_column(): UserDefinedFunction = {
    udf[String, Long]( lookupColName => FN_set_nom_on_column(lookupColName))
  }

  /**
    * Lógica para asignar un "si" o un "no" en función de si el valor de la columna a mirar, es un 1 o un 0
    * @param lookupColValue: Valor de la columna recibida por la UDF
    * @return
    */
  def FN_set_nom_on_column(lookupColValue : java.lang.Long): String = {

    var result = "no"

    if(lookupColValue == 1){
      result = "si"
    }

    result
  }

}
