package es.pue.mediaset.share

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties, TimeZone}

import org.apache.commons.cli.{BasicParser, HelpFormatter, Options, ParseException}

/**
  * Clase que gestiona los argumentos introducidos por linea de comandos
  *
  * @param args Argumentos del usuario
  */
class Entity(parametrizationCfg: Properties, table : String, isInputTable : Boolean = false) extends Serializable {

  private val name : String = parametrizationCfg.getProperty("mediaset.tbl.name." + table, table)
  private val db : String =  parametrizationCfg.getProperty("mediaset.tbl.db." + table, getDefaultDB(parametrizationCfg, isInputTable))
  private val location : String = parametrizationCfg.getProperty("mediaset.tbl.location."  + table, getDefaultLocation(parametrizationCfg, table))
  private val format : String = parametrizationCfg.getProperty("mediaset.tbl.format." +  table, getDefaultFormat(parametrizationCfg))
  private val compression : String = parametrizationCfg.getProperty("mediaset.tbl.compression." +  table, getDefaultCompression(parametrizationCfg))

  private def getDefaultDB(parametrizationCfg : Properties, isInputTable : Boolean) : String = {

    if(isInputTable){
      parametrizationCfg.getProperty("mediaset.default.db.input")
    }else{
      parametrizationCfg.getProperty("mediaset.default.db.output")
    }

  }

  private def getDefaultFormat(parametrizationCfg: Properties) : String = {

    parametrizationCfg.getProperty("mediaset.default.tbl.format")

  }

  private def getDefaultCompression(parametrizationCfg: Properties) : String = {

    parametrizationCfg.getProperty("mediaset.default.tbl.compression")

  }

  private def getDefaultLocation(parametrizationCfg: Properties, table : String) : String = {

    var defaultLocation : String = parametrizationCfg.getProperty("mediaset.default.tbl.location")

    if(defaultLocation.endsWith("/")){
      defaultLocation = defaultLocation.dropRight(1) + s"/$table/"
    }else{
      defaultLocation = defaultLocation + s"/$table/"
    }

    defaultLocation
  }

  def getName : String = name
  def getDB : String = db
  def getLocation : String = location
  def getFormat : String = format
  def getCompression : String = compression
  def getDBTable : String = db + "." + name

  override def toString = s"$name"

}