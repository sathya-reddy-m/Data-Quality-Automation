
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.{lit}
import org.apache.log4j.{LogManager, Logger, BasicConfigurator}
import org.rogach.scallop._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import java.text.SimpleDateFormat

import scala.util.matching.Regex


import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}

import zone_global.{QualityCheckerGlobal, FindFaillingRowsGlobal}

import utils.{QualityResultParser, BuildChecks, BuildRowChecker, ConfigParser}


class QualityCheckerGlobalArguments(arguments: Seq[String]) extends ScallopConf(arguments) {
  val storage_input_file = opt[String](required = true)
  val storage_output_dir = opt[String](required = true)
  val storage_account_name = opt[String](required = false)
  val storage_container_name = opt[String](required = false)
  val storage_account_access_key = opt[String](required = false)
  val save_failing_rows = opt[Boolean]()
  val config_file = opt[String](required=true)
  verify()
}



object QualityCheckerGlobalMain {
    val log: Logger = LogManager.getLogger(QualityCheckerGlobalMain.getClass)
    
    def main(args: Array[String]) : Unit={

        BasicConfigurator.configure()
        
        // Parse arguments
        val parsed_args = new QualityCheckerGlobalArguments(args)
        val save_failing_rows = parsed_args.save_failing_rows.getOrElse(false).asInstanceOf[Boolean]
        val configFile = parsed_args.config_file.getOrElse(null).asInstanceOf[String]
        val STORAGE_INPUT_FILE = parsed_args.storage_input_file.getOrElse(null).asInstanceOf[String]
        val STORAGE_OUTPUT_DIR = parsed_args.storage_output_dir.getOrElse(null).asInstanceOf[String]
        val STORAGE_ACCOUNT_NAME = parsed_args.storage_account_name.getOrElse(null).asInstanceOf[String] //"b2bfilemgmtsagbqa"
        val STORAGE_CONTAINER_NAME = parsed_args.storage_container_name.getOrElse(null).asInstanceOf[String] //"recommender"
        val STORAGE_ACCOUNT_ACCESS_KEY = parsed_args.storage_account_access_key.getOrElse(null).asInstanceOf[String] //"kP+142HGNObuUz2Jp/yNLNaIRRVOepO2ClPR8/kTqK2bREUrHA4RDJZYT89DVJVdHrSL2onxOggjVLAzcXBfgg=="
        
        var filename = STORAGE_INPUT_FILE // processed/ZA_accounts_20191024.csv        
        
        // val dateFormatted = STORAGE_INPUT_FILE.split("_").last.split("\\.")(0)
        val datetimeNowStr = STORAGE_INPUT_FILE.split("_").last.split("\\.")(0)
        // val dateInputFormat = new SimpleDateFormat("yyyyMMdd")
        // val dateOutputFormat = new SimpleDateFormat("yyyy-MM-dd")
        // val dateFormatted = dateOutputFormat.format(dateInputFormat.parse(datetimeNowStr))
        
        // Parse config file
        val config = ConfigParser.parse(configFile)
        
        // log.info(config)
        val tableName = config("entity_name").asInstanceOf[String]
        
        val outputBaseFilename = filename.split("/").last.split("\\.")(0)
        var outputFilename = "%s/%s/%s/%s_stats".format(STORAGE_OUTPUT_DIR,tableName,datetimeNowStr,outputBaseFilename)
        var outputFilenameFailingRows = "%s/%s/%s/%s_failing_rows".format(STORAGE_OUTPUT_DIR,tableName,datetimeNowStr,outputBaseFilename)

        // Build check rules        
        val rules = config("rules").asInstanceOf[Map[String,Map[String,Any]]]
        val checks = BuildChecks.build(rules)                

        val conf = new SparkConf().setAppName("Check Global").set("spark.executor.heartbeatInterval", "600000").set("spark.network.timeout", "600000")
        val sc = SparkContext.getOrCreate(conf)
        val spark = SparkSession.builder.getOrCreate()
        sc.setLogLevel("ERROR") // https://docs.databricks.com/jobs.html#jar-job-tips
        
        if (STORAGE_ACCOUNT_NAME != null && STORAGE_CONTAINER_NAME != null && STORAGE_ACCOUNT_ACCESS_KEY != null){
            spark.conf.set("fs.azure.account.key.%s.blob.core.windows.net".format(STORAGE_ACCOUNT_NAME), STORAGE_ACCOUNT_ACCESS_KEY)
            sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
            sc.hadoopConfiguration.set("fs.azure.account.key"+STORAGE_ACCOUNT_NAME+"blob.core.windows.net", STORAGE_ACCOUNT_ACCESS_KEY)        
            filename = "wasbs://%s@%s.blob.core.windows.net/%s".format(STORAGE_CONTAINER_NAME, STORAGE_ACCOUNT_NAME, STORAGE_INPUT_FILE)            
            outputFilename = "wasbs://%s@%s.blob.core.windows.net/%s".format("data-quality", STORAGE_ACCOUNT_NAME, outputFilename)
            outputFilenameFailingRows = "wasbs://%s@%s.blob.core.windows.net/%s".format("data-quality", STORAGE_ACCOUNT_NAME, outputFilenameFailingRows)
        }

        // Read input data
        val df = spark.read.format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .option("delimiter", "|")
            .load(filename)
        
        // Check data quality measures
        val qc = new QualityCheckerGlobal
        val qualityResult = qc.do_check(df,checks)
        val parsedQualityResultDF = QualityResultParser.parse_results(qualityResult, spark)
        
        parsedQualityResultDF
            .withColumn("QualityCheckDate", lit(datetimeNowStr))
            .repartition(1)
            .write.format("csv")
            .mode("overwrite")
            .option("header", "true")
            .save(outputFilename)
            
        // Find failing rows
        if (save_failing_rows){
            val fr = new FindFaillingRowsGlobal
            val rowCheckers = BuildRowChecker.build(rules)
            
            val failingRows = fr.find(df,spark,sc, rowCheckers)

            failingRows
                .withColumn("QualityCheckDate", lit(datetimeNowStr))
                .repartition(1)
                .write.format("csv")
                .mode("overwrite")
                .option("header", "true")
                .save(outputFilenameFailingRows)                
        }

        
    }
    
}