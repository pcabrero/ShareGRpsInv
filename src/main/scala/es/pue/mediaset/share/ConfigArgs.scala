package es.pue.mediaset.share

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import org.apache.commons.cli.{BasicParser, HelpFormatter, Options, ParseException}

/**
  * Clase que gestiona los argumentos introducidos por linea de comandos
  *
  * @param args Argumentos del usuario
  */
class ConfigArgs(args: Array[String]) extends Serializable {


  private val utils = new Utils()

  // Valores de las opciones
  // tupla (opt, longOpt)
  // Por ejemplo ("h", "help") corresponde a -h y --help
  private val OPT_HELP = ("h", "help")
  private val OPT_INITIAL_PROCESS_MONTH = ("pm", "process-initial-month")
  private val OPT_END_PROCESS_MONTH = ("pm", "process-end-month")
  private val OPT_PARAMETRIZATION_FILE_NAME = (null, "parametrization-filename")
  private val OPT_PRO = (null, "pro")
  private val OPT_PROCESS = (null, "process")

  // Valores por defecto
  private var MES_EN_CURSO = ""
  private var YEAR_INITIAL_MONTH = "1999-01"
  private var YEAR_END_MONTH = "2199-12"
  private var help = false
//  private var MES_EN_CURSO_POR_DEFECTO: String  = ""
  private var PARAMETRIZATION_FILE_NAME = ""
  private var PRO = "pre"
  private var PROCESS : String = null

  // Argumentos aceptados por linea de comandos
  private val options = new Options()
  options.addOption(OPT_HELP._1, OPT_HELP._2, false,
    "Mostrar la ayuda")

  options.addOption(OPT_INITIAL_PROCESS_MONTH._1, OPT_INITIAL_PROCESS_MONTH._2, true,
    "Introduce el mes inicial a procesar, por defecto sera el mes en curso o mes vivo")

  options.addOption(OPT_END_PROCESS_MONTH._1, OPT_END_PROCESS_MONTH._2, true,
    "Introduce el mes final a procesar, por defecto sera el mes en curso o mes vivo")

  options.addOption(OPT_PARAMETRIZATION_FILE_NAME._1, OPT_PARAMETRIZATION_FILE_NAME._2, true,
    "Introduce el nombre del fichero properties. Parametro obligatorio para la ejecución del proceso de PrepararDatos")

  options.addOption(OPT_PRO._1, OPT_PRO._2, false,
    "Establece el entorno de ejecución")

  options.addOption(OPT_PROCESS._1, OPT_PROCESS._2, true,
    "Selecciona el proceso a ejecutar")

  // Procesar argumentos de linea de comandos
  try {
    this.parse(args)


    // Comprobar si hay que mostrar la ayuda por linea de comandos
    if (this.mostrarAyuda) {
      this.printHelp()
      System.exit(0)
    }
  }
  catch {
    // Error en los parametros de la linea de comandos
    case _: ParseException =>
      // Mostrar la ayuda por linea de comandos
      this.printHelp()
      System.exit(0)
  }

  private def parse(args: Array[String]): Unit = {
    // Parsear argumentos
    val parser = new BasicParser()
    val cmd = parser.parse(options, args)

    // Recuperar valores de los argumentos
    // Si no existen se deja el valor por defecto (si se contempla)
    if (cmd.hasOption(OPT_HELP._1) && args.length == 1) help = true

    if (cmd.hasOption(OPT_INITIAL_PROCESS_MONTH._2))   YEAR_INITIAL_MONTH = cmd.getOptionValue(OPT_INITIAL_PROCESS_MONTH._2)

    if (cmd.hasOption(OPT_END_PROCESS_MONTH._2))   YEAR_END_MONTH = cmd.getOptionValue(OPT_END_PROCESS_MONTH._2)

    if (cmd.hasOption(OPT_PARAMETRIZATION_FILE_NAME._2)) PARAMETRIZATION_FILE_NAME = cmd.getOptionValue(OPT_PARAMETRIZATION_FILE_NAME._2)

    if (cmd.hasOption(OPT_PRO._2)) PRO = "pro"

    if (cmd.hasOption(OPT_PROCESS._2)) PROCESS = cmd.getOptionValue(OPT_PROCESS._2)

  }

  def mostrarAyuda: Boolean = help

  def getInitialMonth: String = YEAR_INITIAL_MONTH

  def getEndMonth: String = YEAR_END_MONTH

  def getProcessMonth: String = YEAR_INITIAL_MONTH

  def getParametrizationFileName: String = PARAMETRIZATION_FILE_NAME

  def getEnvironment : String = PRO

  def printHelp(): Unit = {
    val formatter = new HelpFormatter()
    formatter.setWidth(160)
    formatter.setDescPadding(5)

    val usage = s"spark2-submit --master yarn --deploy-mode cluster --class es.pue.mediaset.share.Main"

    formatter.printHelp(usage, options)
  }

  def getFechaActual: String = {
    val tz: TimeZone = TimeZone.getTimeZone("Europe/Madrid")
    val sdf_yyyymmdd = new SimpleDateFormat("yyyy-MM")
    sdf_yyyymmdd.setTimeZone(tz)
    sdf_yyyymmdd.format(Calendar.getInstance(tz).getTime)
  }

  def getProcess : String = PROCESS

}