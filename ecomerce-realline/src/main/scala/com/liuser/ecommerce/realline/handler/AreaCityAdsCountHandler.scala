package com.liuser.ecommerce.realline.handler

import java.util.Properties

import com.liuser.Ecommerce.common.utils.PropertiesUtil
import com.liuser.ecommerce.realline.bean.AdsLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object AreaCityAdsCountHandler {

    //需求五
    def handle(adsLogDstream: DStream[AdsLog])={

        //整理dstreanm 为  kv结构  (date:area:city:ads,1L)
        val adsClickDsteam: DStream[(String, Long)] = adsLogDstream.map { adslog =>

            val key: String = adslog.getDate() + ":" + adslog.area + ":" + adslog.city + ":" + adslog.adsId
            (key, 1L)
        }

        //利用updateStateByKey  进行累加
        val adsClickCountDstream: DStream[(String, Long)] = adsClickDsteam.updateStateByKey { (countSeq: Seq[Long], total: Option[Long]) =>

            //把countSeq 汇总  累加在total中
            val countSum: Long = countSeq.sum
            val curTotal: Long = total.getOrElse(0L) + countSum

            Some(curTotal)
        }
        //把结果存到redis
        adsClickCountDstream.foreachRDD{rdd =>
            val prop: Properties = PropertiesUtil.load("config.properties")

            rdd.foreachPartition{ adsClickCountItr=>

                //建立连接
                val jedis = new Jedis(prop.getProperty("redis.host"),prop.getProperty("redis.port").toInt)
                adsClickCountItr.foreach{ case (key,count) =>
                jedis.hset("date:area:city:ads",key,count.toString)
                }
                jedis.close()
            }

        }
    }
}
