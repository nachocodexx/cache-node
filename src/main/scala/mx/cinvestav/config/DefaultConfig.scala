package mx.cinvestav.config

case class KeyStoreInfo(exchange:String, routingKey:String)

case class LoadBalancerInfo(
                           strategy:String,
                           exchange:String,
                           routingKey:String
                           )

case class DefaultConfigV5(
                            nodeId:String,
                            loadBalancer:LoadBalancerInfo,
                            replicationFactor:Int,
                            poolId:String,
                            storagePath:String,
                            cacheNodes:List[String],
                            cachePolicy:String,
                            cacheSize:Int,
                            rabbitmq: RabbitMQClusterConfig,
                            port:Int,
                            host:String,
                            replicationStrategy:String,
                            keyStore:KeyStoreInfo,
                            syncNodes:List[String],
                            clouds:List[String],
                            level:Int=0
                            //                          sourceFolders:List[String]
                        )

