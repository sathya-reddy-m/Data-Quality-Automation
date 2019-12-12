package utils

import org.apache.spark.{SparkContext}
import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.functions.udf

import com.amazon.deequ.VerificationResult
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}

import scala.util.matching.Regex
import scala.util.parsing.json._
import scala.io.Source


case class ParsedResult(column_name: String, constrain: String, status: String, constrain_type: String, coverage: Double)

abstract class RowChecker(columnName: String, 
                            isNullable: Boolean)

case class RowCheckerString(columnName: String,
                                isNullable: Boolean, 
                                matches: Option[String]=None, 
                                minLength: Option[Int]=None, 
                                maxLength: Option[Int]=None) extends RowChecker(columnName,isNullable)

case class RowCheckerInt(columnName: String, 
                            isNullable: Boolean,
                            minValue: Option[Int]=None,
                            maxValue: Option[Int]=None) extends RowChecker(columnName,isNullable)


abstract class AbstractQualityChecker {
    def do_check(data_df: DataFrame) : VerificationResult
}

abstract class AbstractFindFaillingRows {
    def find(data_df: DataFrame, spark: SparkSession, sc: SparkContext) : DataFrame    

    val failingString = udf((value: String,
                            columnName : String,
                            isNullable: Boolean, 
                            matches: String, 
                            minLength: Int,
                            maxLength: Int) => {
            
            
            var reason = ""
            var errorCount = 0
            
            val matchRegex = matches match {
                case "" => new Regex("(.*?)")
                case s:String => new Regex(matches)
            }

            if ((isNullable == false) && (value == null)) {
                errorCount = errorCount + 1
                reason = reason + " %s - is null".format(errorCount)
            }            

            if (errorCount == 0){
                reason = value match {
                    case matchRegex(_*) => {
                        reason
                    }
                    case _ => {
                        errorCount = errorCount + 1
                        reason = reason + " %s does not match the following regular expression: %s".format(errorCount, matches)
                        reason
                    }
                }
                
                if (value != null){
                    if (value.length < minLength) {
                        errorCount = errorCount + 1
                        reason = reason + " %s has less than %s charaters".format(errorCount, minLength)
                    }
                }

                if (value != null){
                    if (value.length > maxLength) {
                        errorCount = errorCount + 1
                        reason = reason + " %s has more than %s charaters".format(errorCount, maxLength)
                    }
                }
            }
            reason
        })

        val failingInt = udf((value: String,
                            columnName : String,
                            isNullable: Boolean,
                            minValue: Int,
                            maxValue: Int) => {
            
            
            var reason = ""
            var errorCount = 0
                        

            if ((isNullable == false) && (value == null)) {
                errorCount = errorCount + 1
                reason = reason + " %s - is null".format(errorCount)
                reason
            }

            if (errorCount == 0){                                
                if (value != null){
                    if (value.length < minValue) {
                        errorCount = errorCount + 1
                        reason = reason + " %s is less than %s ".format(errorCount, minValue)
                    }
                }

                if (value != null){
                    if (value.length > maxValue) {
                        errorCount = errorCount + 1
                        reason = reason + " %s is greater than %s ".format(errorCount, maxValue)
                    }
                }
            }
            reason
        })
}


object ConfigParser{
    def parse(configFile:String) : Map[String,Any] = {
        return JSON.parseFull(Source.fromInputStream(getClass().getResourceAsStream(configFile)).mkString).getOrElse(null).asInstanceOf[Map[String,Any]]
    }
}

/**
 * @var		object	QualityResultParser
 * @global
 */
object QualityResultParser{
    /**
     * parse_results.
     *
     * @author	Rodrigo Pereira
     * @since	v0.0.1
     * @version	v1.0.0	Tuesday, December 3rd, 2019.
     * @param	quality_check_results	:VerificationResult	
     * @param	spark                	:SparkSession      	
     * @return	mixed
     */
    def parse_results(quality_check_results :VerificationResult, spark :SparkSession): DataFrame = {
        val resultsForAllConstraints = quality_check_results.checkResults.flatMap { case (_, checkResult) => checkResult.constraintResults }
        val parsedResults = resultsForAllConstraints.map(r => ParsedResult(r.metric.getOrElse(null).instance, 
                                                                        r.constraint.toString(), 
                                                                        r.status.toString(), 
                                                                        r.metric.getOrElse(null).name, 
                                                                        r.metric.getOrElse(null).value.getOrElse(null).asInstanceOf[Double]  ) )
        return spark.createDataFrame(parsedResults.toSeq)
    }
}

/**
 * @var		object	BuildChecks
 * @global
 */
object BuildChecks{
    /**
     * build.
     *
     * @author	Rodrigo Pereira
     * @since	v0.0.1
     * @version	v1.0.0	Tuesday, December 3rd, 2019.
     * @param	mixed	stats_rule	
     * @return	mixed
     */
    def build(rules: Map[String, Map[String,Any]]): Check = {
        var checks = Check(CheckLevel.Error, "Data unit test")
        val columnList = rules.keys.toList

        for (columnName <- columnList){
            val columnType = rules(columnName).get("type").orElse(Some("string")).get.asInstanceOf[String]
            for (constraint <- rules(columnName)("constraints").asInstanceOf[List[Any]]){
                constraint match  {
                    case constraintName: String =>{
                        constraintName match {
                           case "isComplete" => {
                               checks = checks.isComplete(columnName)
                           }
                           case "isUnique" =>{
                               checks = checks.isUnique(columnName)
                           }
                           
                        }
                    }
                    case constraintMap: Map[String,Any] =>{
                        val constraintName = constraintMap.keys.toList(0)
                        constraintName match {
                            case "hasPattern" =>{
                                val pattern = new Regex(constraintMap.asInstanceOf[Map[String,String]](constraintName))
                                checks = checks.hasPattern(columnName,pattern)
                            }
                            case "isContainedIn" =>{
                                columnType match{
                                    case "string" =>{
                                        val list = constraintMap.asInstanceOf[Map[String,List[String]]](constraintName).toArray
                                        checks = checks.isContainedIn(columnName,list)
                                    }
                                    case "double" =>{
                                        val bounds = constraintMap.asInstanceOf[Map[String,Map[String,Double]]](constraintName)
                                        val lowerBound = bounds.get("lowerBound").orElse(Some(Double.MinValue)).get.asInstanceOf[Double]
                                        val upperBound = bounds.get("upperBound").orElse(Some(Double.MaxValue)).get.asInstanceOf[Double]
                                        checks = checks.isContainedIn(columnName,lowerBound,upperBound)
                                    } 
                                }
                                
                            }
                        }
                    }
                }

            }
        }

        return checks
    }
}

object BuildRowChecker{
    /**
     * build.
     *
     * @author	Rodrigo Pereira
     * @since	v0.0.1
     * @version	v1.0.0	Tuesday, December 3rd, 2019.
     * @param	mixed	stats_rule	
     * @return	mixed
     */
    def build(rules: Map[String, Map[String,Any]]): List[RowChecker] = {
        var constraints: List[RowChecker] = List()
        val columnList = rules.keys.toList

        for (columnName <- columnList){

            var matches:Option[String] = None            
            var minValue:Option[Int] = None
            var maxValue:Option[Int] = None
            var isNullable = true

            val columnType = rules(columnName).get("type").orElse(Some("string")).get.asInstanceOf[String]
            
            for (constraint <- rules(columnName)("constraints").asInstanceOf[List[Any]]){
                constraint match  {
                    case constraintName: String =>{
                        constraintName match {
                           case "isComplete" => {
                               isNullable=false
                           }
                           case _ =>{                               
                           }
                           
                        }
                    }
                    case constraintMap: Map[String,Any] =>{
                        val constraintName = constraintMap.keys.toList(0)
                        constraintName match {
                            case "hasPattern" =>{
                                val pattern = constraintMap.asInstanceOf[Map[String,String]](constraintName)
                                matches = Some(pattern)
                            }
                            case "isContainedIn" =>{
                                columnType match{
                                    case "string" =>{
                                        val pattern = "("+constraintMap.asInstanceOf[Map[String,List[String]]](constraintName).toArray.mkString("|")+")"
                                        matches = Some(pattern)
                                    }
                                    case "double" =>{
                                        val bounds = constraintMap.asInstanceOf[Map[String,Map[String,Double]]](constraintName)
                                        minValue = Some(Math.round(bounds.get("lowerBound").orElse(Some(Double.MinValue)).get.asInstanceOf[Double]).asInstanceOf[Int])
                                        maxValue = Some(Math.round(bounds.get("upperBound").orElse(Some(Double.MaxValue)).get.asInstanceOf[Double]).asInstanceOf[Int])
                                    } 
                                }
                            }
                        }
                    }
                }
            }

            columnType match {
                case "string" => {
                    constraints = RowCheckerString(columnName=columnName,isNullable=isNullable, matches=matches, minLength=minValue, maxLength=maxValue) :: constraints
                }
                case "double" => {
                    constraints = RowCheckerInt(columnName=columnName,isNullable=isNullable, minValue=minValue, maxValue=maxValue) :: constraints
                }
            }
            
        }

        

        return constraints
    }
}



