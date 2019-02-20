package com.liuser.ecommerce.offline.handler

import com.liuser.Ecommerce.common.bean.UserVisitAction
import com.liuser.Ecommerce.common.utils.JdbcUtil
import com.liuser.ecommerce.offline.bean.CategoryCount
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CategoryTopSessionHandler {

    def handle(sparkSession: SparkSession, userVisitActionRDD: RDD[UserVisitAction], taskId: String, top10CategoryList: List[CategoryCount]) = {

        val top10Cid: List[Long] = top10CategoryList.map(_.categoryId.toLong)

        // 把top10Cid 做成广播变量
        val top10CidBC: Broadcast[List[Long]] = sparkSession.sparkContext.broadcast(top10Cid)
        //过滤
        val filterUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter { userVisitAction =>
            top10CidBC.value.contains(userVisitAction.click_category_id)
        }


        //    2   RDD[UserVisitAction] 统计次数    得到每个session 点击 top10品类的次数
        //      rdd->k-v结构  .map(action.category_click_id+"_"+action.sessionId,1L)
        //    ->.reducebykey(_+_)
        //    ->RDD[action.category_click_id+"_"+action.sessionID,count]
        val clickCountGroupByCidSessionRDD: RDD[(String, Long)] = filterUserVisitActionRDD.map(action => (action.click_category_id + "_" + action.session_id, 1L)).reduceByKey(_+_)

        //    3  分组 准备做组内排序  以品类id  分组
        val sessionCountGroupbyCidRdd: RDD[(String, Iterable[(String, Long)])] = clickCountGroupByCidSessionRDD.map { case (sessionCid, count) =>
            val cidSessionArr: Array[String] = sessionCid.split("_")
            val cid: String = cidSessionArr(0)
            val sessionId: String = cidSessionArr(1)
            (cid, (sessionId, count))
        }.groupByKey()

        //  4 小组赛  保留每组的前十名
        val sessionTop10RDD: RDD[Array[Any]] = sessionCountGroupbyCidRdd.flatMap { case (cid, sessionItr) =>
            //倒序取前十
            val sessionTop10List: List[(String, Long)] = sessionItr.toList.sortWith { (sessionCount1, sessionCount2) =>
                sessionCount1._2 > sessionCount2._2
            }.take(10)
            // 调整结构按照最终要保存的结构 填充session信息
            val sessionTop10ListWithCidList = sessionTop10List.map { case (sessionId, clickcount) =>
                Array(taskId, cid, sessionId, clickcount)
            }
            sessionTop10ListWithCidList
        }

        val sessionTop10Arr: Array[Array[Any]] = sessionTop10RDD.collect()

        //保存到mysql
        JdbcUtil.executeBatchUpdate("insert into category_top10_session_top10 values(?,?,?,?)" ,sessionTop10Arr)


    }
}
