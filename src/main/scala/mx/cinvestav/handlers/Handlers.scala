package mx.cinvestav.handlers

import cats.effect.IO
import dev.profunktor.fs2rabbit.model.{AmqpFieldValue, QueueName}
import mx.cinvestav.CommandHandlers
import mx.cinvestav.Declarations.{CommandIds, NodeContextV5}
import mx.cinvestav.utils.v2.Acker

object Handlers {

  def apply(queueName: QueueName)(implicit ctx:NodeContextV5): IO[Unit] = {
    val rabbitMQContext = ctx.rabbitMQContext
    val connection      = rabbitMQContext.connection
    val client          = rabbitMQContext.client
    client.createChannel(connection) .use { implicit channel =>
      for {
        _ <- ctx.logger.debug("START CONSUMING")
        (_acker, consumer) <- ctx.rabbitMQContext.client.createAckerConsumer(queueName = queueName)
        _ <- consumer.evalMap { implicit envelope =>
          val maybeCommandId = envelope.properties.headers.get("commandId")
          implicit val acker: Acker = Acker(_acker)
          maybeCommandId match {
            case Some(commandId) => commandId match {
              //              case AmqpFieldValue.StringVal(value) if value == CommandIds.PROPOSE => CommandHandlers.propose()
              case AmqpFieldValue.StringVal(value) if value == CommandIds.REPLICATE => ReplicateHandler()
              case AmqpFieldValue.StringVal(value) if value == CommandIds.PULL => PullHandler()
              case AmqpFieldValue.StringVal(value) if value == CommandIds.PROPOSE => ProposeHandler()
              case AmqpFieldValue.StringVal(value) if value == CommandIds.PREPARE => PrepareHandler()
              case AmqpFieldValue.StringVal(value) if value == CommandIds.PROMISE => PromiseHandler()
              case AmqpFieldValue.StringVal(value) if value == CommandIds.ADD_NODE => CommandHandlers.addNode()
              case AmqpFieldValue.StringVal(value) if value == CommandIds.REMOVE_NODE=> CommandHandlers.removeNode()
              case x =>
                ctx.logger.error(s"NO COMMAND_HANDLER FOR $x") *> acker.reject(envelope.deliveryTag)
            }
            case None => for{
              _ <- ctx.logger.error("NO COMMAND_ID PROVIED")
              _ <- acker.reject(envelope.deliveryTag)
            } yield ()
          }
        }.compile.drain
      } yield ()
    }
  }


}
