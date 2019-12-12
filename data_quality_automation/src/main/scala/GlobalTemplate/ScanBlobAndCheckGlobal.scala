import java.net.URI

import com.microsoft.azure.storage.{StorageUri, StorageCredentialsAccountAndKey}
import com.microsoft.azure.storage.blob.{CloudBlobContainer,CloudBlobClient}

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.{SparkSession}
import org.apache.log4j.{LogManager, Logger, BasicConfigurator}

import org.rogach.scallop._

import QualityCheckerGlobalMain._

import utils.{ConfigParser}

import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global


class ScanAndCheckGlobalArguments(arguments: Seq[String]) extends ScallopConf(arguments) {
  val storage_input_dir = opt[String](required = true)
  val storage_account_name = opt[String](required = true)
  val storage_container_name = opt[String](required = true)
  val storage_account_access_key = opt[String](required = true)
  val save_failing_rows = opt[Boolean]()
  val file_prefix = opt[String](required=true)
  val config_file = opt[String](required=true)
  verify()
}


object ScanAndCheckGlobal {
    val log: Logger = LogManager.getLogger(ScanAndCheckGlobal.getClass)
    
    def main(args: Array[String]) : Unit={

        BasicConfigurator.configure()

        val parsed_args = new ScanAndCheckGlobalArguments(args)
        val configFile = parsed_args.config_file.getOrElse(null).asInstanceOf[String]
        val filePrefix = parsed_args.file_prefix.getOrElse(null).asInstanceOf[String]
        val save_failing_rows = parsed_args.save_failing_rows.getOrElse(false).asInstanceOf[Boolean]
        val STORAGE_INPUT_DIR = parsed_args.storage_input_dir.getOrElse(null).asInstanceOf[String]        
        val STORAGE_ACCOUNT_NAME = parsed_args.storage_account_name.getOrElse(null).asInstanceOf[String] //"b2bfilemgmtsagbqa"
        val STORAGE_CONTAINER_NAME = parsed_args.storage_container_name.getOrElse(null).asInstanceOf[String] //"recommender"
        val STORAGE_ACCOUNT_ACCESS_KEY = parsed_args.storage_account_access_key.getOrElse(null).asInstanceOf[String] //"kP+142HGNObuUz2Jp/yNLNaIRRVOepO2ClPR8/kTqK2bREUrHA4RDJZYT89DVJVdHrSL2onxOggjVLAzcXBfgg=="
        val inputStorageContanerURI = "https://%s.blob.core.windows.net/%s".format(STORAGE_ACCOUNT_NAME,STORAGE_CONTAINER_NAME)
        val outputStorageContanerURI = "https://%s.blob.core.windows.net/%s".format(STORAGE_ACCOUNT_NAME,"data-quality")
        
        val conf = new SparkConf().setAppName("Scan And Check Global").set("spark.executor.heartbeatInterval", "600000").set("spark.network.timeout", "600000")
        val sc = SparkContext.getOrCreate(conf)
        val spark = SparkSession.builder.getOrCreate()

        sc.setLogLevel("ERROR") // https://docs.databricks.com/jobs.html#jar-job-tips

        val config = ConfigParser.parse(configFile)
        // val tableName = config("entity_name").asInstanceOf[String]
        val zone = config("zone").asInstanceOf[String]

        // Build Credentials and Containers        
        val blobCredentials = new StorageCredentialsAccountAndKey(STORAGE_ACCOUNT_NAME,STORAGE_ACCOUNT_ACCESS_KEY)
        
        val inputContainerURI = new StorageUri(new URI(inputStorageContanerURI))
        val inputBlobClient = new CloudBlobClient(inputContainerURI,blobCredentials)
        // val inputContainer = new CloudBlobContainer(inputContainerURI,inputBlobClient)
        val inputContainer = new CloudBlobContainer(inputContainerURI,blobCredentials)
        
        val outputContainerURI = new StorageUri(new URI(outputStorageContanerURI))
        val outputBlobClient = new CloudBlobClient(outputContainerURI,blobCredentials)
        // val outputContainer = new CloudBlobContainer(outputContainerURI,outputBlobClient)
        val outputContainer = new CloudBlobContainer(outputContainerURI,blobCredentials)

        val prefix = "%s/%s".format(STORAGE_INPUT_DIR, filePrefix)
        val blobList =  inputContainer.listBlobs(prefix,true, null, null, null).iterator()

        var argsList: List[Array[String]] = List[Array[String]]()

        while (blobList.hasNext()) {
            val blobFilename = blobList.next().getUri().toString()
            val blobBasename = "%s/%s".format(STORAGE_INPUT_DIR,blobFilename.split("/").last)            
            // log.info("Processing %s".format(blobBasename))

            var qcArgs:Array[String] = null
            if (save_failing_rows){
                qcArgs = Array("--storage_input_file", blobBasename,
                                        "--storage_account_name", STORAGE_ACCOUNT_NAME,
                                        "--storage_container_name", STORAGE_CONTAINER_NAME,
                                        "--storage_account_access_key", STORAGE_ACCOUNT_ACCESS_KEY,
                                        "--storage_output_dir", "%s/".format(zone),
                                        "--config_file", configFile, 
                                        "--save_failing_rows")
            }else{
                qcArgs = Array("--storage_input_file", blobBasename,
                                        "--storage_account_name", STORAGE_ACCOUNT_NAME,
                                        "--storage_container_name", STORAGE_CONTAINER_NAME,
                                        "--storage_account_access_key", STORAGE_ACCOUNT_ACCESS_KEY,
                                        "--storage_output_dir", "%s/".format(zone),
                                        "--config_file", configFile)
            }         

            argsList = qcArgs :: argsList
            // QualityCheckerGlobalMain.main(qcArgs)
            // Thread.sleep(1000)
        }


        var pool = 0
        
        def poolId = {
            pool = pool + 1
            pool
        }
        
        def runner(qcArgsThread: Array[String]) = Future {
            sc.setLocalProperty("spark.scheduler.pool", poolId.toString)
            QualityCheckerGlobalMain.main(qcArgsThread)
            // println(qcArgsThread.mkString(" "))
        }
        
        val futures = argsList map(i => runner(i))

        // now you need to wait all your futures to be completed
        futures foreach(f => Await.ready(f, Duration.Inf))

        
    }
}