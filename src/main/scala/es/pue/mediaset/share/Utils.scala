package es.pue.mediaset.share

import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties, TimeZone}

class Utils {

  /**
    *
    * @param path
    * @return
    */
  def loadPropertiesFromPath(path : String): Properties = {
    val file = new File(path)
    val fileInput = new FileInputStream(file)
    val properties = new Properties()
    properties.load(fileInput)
    properties
  }

  /**
    * Recuperar la fecha actual en formato yyyyMMdd
    * @return Fecha actual en formato yyyyMMdd
    */
  def getFechaActual: String = {
    val tz: TimeZone = TimeZone.getTimeZone("Europe/Madrid")
    val sdf_yyyymmdd = new SimpleDateFormat("yyyy-MM")
    sdf_yyyymmdd.setTimeZone(tz)
    sdf_yyyymmdd.format(Calendar.getInstance(tz).getTime)
  }

  // TODO create method to validate date from string
  /**
    * Recuperar la fecha actual en formato yyyyMMdd
    * @return Fecha actual en formato yyyyMMdd
    */
//  def getFechaActual(stringDate : String): String = {
//
//  }


}
