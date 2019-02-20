package com.liuser.ecommerce.offline.handler

import com.liuser.Ecommerce.common.bean.UserVisitAction
import com.liuser.Ecommerce.common.utils.JdbcUtil
import com.liuser.ecommerce.offline.acc.CategoryAccumulator
import com.liuser.ecommerce.offline.bean.CategoryCount
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object CategoryCountHandler {
    def handle(sparkSession: SparkSession, userVisitActionRDD: RDD[UserVisitAction], taskId: String):List[CategoryCount] = {
        //定义累加器, 注册累加器
        val accumulator = new CategoryAccumulator
        sparkSession.sparkContext.register(accumulator)

        //遍历RDD  利用累加器进行累加操作
        userVisitActionRDD.foreach { userVisitAction =>
            // key值确定
            if (userVisitAction.click_category_id != -1L) {
                val key: String = userVisitAction.click_category_id + "_click"
                accumulator.add(key)
            } else if (userVisitAction.order_category_ids != null && userVisitAction.order_category_ids.length > 0) {
                val orderCids: Array[String] = userVisitAction.order_category_ids.split(",")
                for (cid <- orderCids) {
                    val key: String = cid + "_order"
                    accumulator.add(key)
                }
            } else if (userVisitAction.pay_category_ids != null && userVisitAction.pay_category_ids.length > 0) {
                val payCids: Array[String] = userVisitAction.pay_category_ids.split(",")

                for (elem <- payCids) {
                    val key: String = elem + "_pay"
                    accumulator.add(key)
                }
            }
        }
        // 拿到累加器的结果
        val categoryMap: mutable.HashMap[String, Long] = accumulator.value
        println(s"categoryMap= ${categoryMap.mkString("\n")}")

        // 把结果转换为 List[CategoryCount]  //模式匹配能够帮助我们把集合的元素直接输入到某个变量中
        val categoryByGroupCidMap: Map[String, mutable.HashMap[String, Long]] = categoryMap.groupBy({ case (key, count) => key.split("_")(0) })
        println(s"categoryByGroupCidMap= ${categoryByGroupCidMap.mkString("\n")}")

        //转成List[CategoryCount]
        val categoryCountList: List[CategoryCount] = categoryByGroupCidMap.map { case (cid, actionMap) =>
            CategoryCount("", cid, actionMap.getOrElse(cid + "_click", 0L), actionMap.getOrElse(cid + "_order", 0L), actionMap.getOrElse(cid + "_pay", 0L))
        }.toList

        //对结果进行排序, 截取
        val sortedCategoryCountList: List[CategoryCount] = categoryCountList.sortWith { (categoryCount1, categoryCount2) =>
            if (categoryCount1.clickCount > categoryCount2.clickCount) {
                true
            } else if (categoryCount1.clickCount == categoryCount2.clickCount) {
                if (categoryCount1.orderCount > categoryCount2.orderCount) {
                    true
                } else {
                    false
                }
            } else {
                false
            }
        }.take(10)

        //把前十保存到mysql

        println((s"sortedCategoryCountList = ${sortedCategoryCountList.mkString("\n")}"))
        val resultList: List[Array[Any]] = sortedCategoryCountList.map { categoryCount =>
            Array(taskId, categoryCount.categoryId,
                categoryCount.clickCount, categoryCount.orderCount, categoryCount.payCount)
        }

        JdbcUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)",resultList)
        sortedCategoryCountList
    }
}
