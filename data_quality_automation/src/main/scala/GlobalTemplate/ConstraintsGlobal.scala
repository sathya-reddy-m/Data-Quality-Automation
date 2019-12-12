package zone_global

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.schema._

import org.apache.spark.{SparkContext}
import org.apache.spark.sql.{SparkSession,DataFrame,Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.matching.Regex
import scala.collection.mutable.Map

import utils.{RowChecker,RowCheckerString,RowCheckerInt}


/**
 * QualityCheckerGlobal.
 *
 * @author	Rodrigo Pereira
 * @since	v0.0.1
 * @version	v1.0.0	Tuesday, December 3rd, 2019.
 * @global
 */
class QualityCheckerGlobal{

    def do_check(data_df :org.apache.spark.sql.DataFrame, checks: Check): com.amazon.deequ.VerificationResult = {
        val verificationResult = VerificationSuite()
            .onData(data_df)
            .addCheck(checks)
            .run()

        return  verificationResult
    }    
}


/**
 * FindFaillingRowsGlobal.
 *
 * @author	Rodrigo Pereira
 * @since	v0.0.1
 * @version	v1.0.0	Tuesday, December 3rd, 2019.
 * @global
 */
class FindFaillingRowsGlobal{

    val failingString = udf((value: String,
                            columnName : String,
                            isNullable: Boolean,
                            matches: String,
                            minLength: Int,
                            maxLength: Int) => {
            
            val matchRegex = matches match {
                case "" => {
                    new Regex("(.*?)")
                }
                case s:String => {
                    new Regex(s)
                }
            }

            var reason = ""
            var errorCount = 0

            if ((isNullable == false) && (value == null)) {
                errorCount = errorCount + 1
                reason = reason + " %s - is null".format(errorCount)
                reason
            }                    

            if (errorCount == 0){
                reason = value match {
                    case matchRegex(_*) => {
                        reason
                    }
                    case _ => {
                        errorCount = errorCount + 1
                        reason = reason + " %s -  does not match the following regular expression: %s".format(errorCount, matches)
                        reason
                    }
                }
                
                if (value != null){
                    if (value.length < minLength) {
                        errorCount = errorCount + 1
                        reason = reason + " %s - has less than %s charaters".format(errorCount, minLength)
                    }
                }

                if (value != null){
                    if (value.length > maxLength) {
                        errorCount = errorCount + 1
                        reason = reason + " %s - has more than %s charaters".format(errorCount, maxLength)
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
                    reason = reason + " %s - is less than %s ".format(errorCount, minValue)
                }
            }

            if (value != null){
                if (value.length > maxValue) {
                    errorCount = errorCount + 1
                    reason = reason + " %s - is greater than %s ".format(errorCount, maxValue)
                }
            }
        }
        reason
    })
    
    def find(data_df :org.apache.spark.sql.DataFrame, spark: SparkSession, sc: SparkContext, constraints: List[RowChecker]): DataFrame = {
        
        var invalidRowsDf = spark.createDataFrame(sc.emptyRDD[Row],data_df.schema)
        invalidRowsDf = invalidRowsDf.withColumn("failing_column",lit(""))
        invalidRowsDf = invalidRowsDf.withColumn("failing_reason",lit(""))

        for (constraint <- constraints){            

            constraint match {
                case c: RowCheckerString =>{
                    

                    val schema = RowLevelSchema().withStringColumn(name=c.columnName,
                                                                    isNullable=c.isNullable,
                                                                    minLength=c.minLength,
                                                                    maxLength=c.maxLength,
                                                                    matches=c.matches)
                    val result = RowLevelSchemaValidator.validate(data_df, schema)
                    var invalidRowsPerConstraint = result.invalidRows
                    invalidRowsPerConstraint = invalidRowsPerConstraint.withColumn("failing_column",lit(c.columnName))
                    invalidRowsPerConstraint = invalidRowsPerConstraint.withColumn("failing_reason", failingString(
                                                                                                        col(c.columnName),
                                                                                                        lit(c.columnName),
                                                                                                        lit(c.isNullable),
                                                                                                        lit(c.matches.getOrElse("")),
                                                                                                        lit(c.minLength.getOrElse(Int.MinValue)),
                                                                                                        lit(c.maxLength.getOrElse(Int.MaxValue))
                                                                                        ))                    
                    
                    invalidRowsDf = invalidRowsDf.union(invalidRowsPerConstraint)
                }

                case c: RowCheckerInt =>{
                    

                    val schema = RowLevelSchema().withIntColumn(name=c.columnName,
                                                                    isNullable=c.isNullable,
                                                                    minValue=c.minValue,
                                                                    maxValue=c.maxValue)
                    val result = RowLevelSchemaValidator.validate(data_df, schema)
                    var invalidRowsPerConstraint = result.invalidRows
                    invalidRowsPerConstraint = invalidRowsPerConstraint.withColumn("failing_column",lit(c.columnName))
                    invalidRowsPerConstraint = invalidRowsPerConstraint.withColumn("failing_reason", failingInt(
                                                                                                        col(c.columnName),
                                                                                                        lit(c.columnName),
                                                                                                        lit(c.isNullable),
                                                                                                        lit(c.minValue.getOrElse(Int.MinValue)),
                                                                                                        lit(c.maxValue.getOrElse(Int.MaxValue))
                                                                                        ))                    
                    
                    invalidRowsDf = invalidRowsDf.union(invalidRowsPerConstraint)
                }
            }
        }
        
        return invalidRowsDf
    }
}