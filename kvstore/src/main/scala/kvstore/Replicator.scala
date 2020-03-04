package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, Props}

import scala.concurrent.duration._
import scala.language.postfixOps

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import context.dispatcher

  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  def receive: Receive = manInTheMiddle(Map.empty, Map.empty)

  /**
    * Model a Replicator acting as a man-in-the-middle between an external actor and `replica`
    * @param acks map from sequence number to pair of sender and request
    * @param ongoingTimers map from a sequence number to a Cancellable corresponding to an ongoing timer
    * @return A partial function handling Replicate and SnapshotAck messages
    */
  private def manInTheMiddle(acks: Map[Long, (ActorRef, Replicate)], ongoingTimers: Map[Long, Cancellable]): Receive = {
    case msg @ Replicate(k, vo, _) =>
      val seq = nextSeq()
      val newAcks = acks updated (seq, (sender, msg))
      val newTimers = ongoingTimers updated (seq, context.system.scheduler scheduleWithFixedDelay (
        initialDelay = 0 millis,
        delay = 100 millis,
        receiver = replica,
        message = Snapshot(k, vo, seq)
      ))
      context become manInTheMiddle(newAcks, newTimers)
    case SnapshotAck(k, seq) =>
      if (ongoingTimers.isDefinedAt(seq) && acks.isDefinedAt(seq)) {
        ongoingTimers(seq).cancel()
        val (recipient, msg) = acks(seq)
        recipient ! Replicated(k, msg.id)
        context become manInTheMiddle(acks removed seq, ongoingTimers removed seq)
      }
  }

}
