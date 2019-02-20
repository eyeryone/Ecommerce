package com.liuser.ecommerce.offline.app

import java.util.{Properties, UUID}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.liuser.Ecommerce.common.bean.UserVisitAction
import com.liuser.Ecommerce.common.utils.PropertiesUtil
import com.liuser.ecommerce.offline.bean.CategoryCount
import com.liuser.ecommerce.offline.handler.{CategoryCountHandler, CategoryTopSessionHandler}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object OffflinApp {

    def main(args: Array[String]): Unit = {

        val taskId: String = UUID.randomUUID().toString

        val sparkConf: SparkConf = new SparkConf().setAppName("Ecommerce-offline").setMaster("local[*]")

        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        //根据条件把hive中的数据读出来
        //得到RDD[UserVisitAction]

        val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionToRDD(sparkSession)

        /*println(userVisitActionRDD)*/
        //需求一
        val categoryCountList: List[CategoryCount] = CategoryCountHandler.handle(sparkSession,userVisitActionRDD,taskId)

        println("任务完成1")
        //需求二
        CategoryTopSessionHandler.handle(sparkSession,userVisitActionRDD,taskId,categoryCountList)
        println("需求二完成")
    }

    def readUserVisitActionToRDD(sparkSession: SparkSession): RDD[UserVisitAction] = {

        //使用json工具解析文件 fastjson  gson jackson
        val properties: Properties = PropertiesUtil.load("conditions.properties")
        val conditionsJson: String = properties.getProperty("condition.params.json")
        val conditionJsonObj: JSONObject = JSON.parseObject(conditionsJson)
        val startDate: String = conditionJsonObj.getString("startDate")
        val endDate: String = conditionJsonObj.getString("endDate")
        val startAge: String = conditionJsonObj.getString("startAge")
        val endAge: String = conditionJsonObj.getString("endAge")

        //联合表的目的是因为,user_visit_action表中没有age, 所以需要关联表
        val sql: StringBuilder = new StringBuilder("select v.* from user_visit_action v,user_info u where v.user_id=u.user_id")
        if (startDate.nonEmpty) {
            sql.append(" and date>='" + startDate + "'")
        }
        if (endDate.nonEmpty){
            sql.append(" and date<='"+ endDate +"'")
        }
        if (startAge.nonEmpty){
            sql.append(" and age>='"+ startAge +"'")
        }
        if (endAge.nonEmpty){
            sql.append(" and age<='"+ endAge +"'")
        }
        println(sql)

        sparkSession.sql("use ecommerce")

        import sparkSession.implicits._
        val rdd: RDD[UserVisitAction] = sparkSession.sql(sql.toString()).as[UserVisitAction].rdd
        rdd
    }
}
