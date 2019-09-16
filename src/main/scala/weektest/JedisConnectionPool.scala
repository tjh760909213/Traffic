package weektest

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool {
  /**
    * 连接池
    */
  val config = new JedisPoolConfig

  config.setMaxTotal(30) //最大连接
  config.setMaxIdle(10) //最大空闲

  val pool = new JedisPool(config,"hadoop01",6379)
  //获取Jedis对象
  def getConnection():Jedis={
    pool.getResource
  }
}
