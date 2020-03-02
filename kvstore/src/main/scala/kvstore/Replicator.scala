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
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // map from a sequence number to a Cancellable corresponding to an ongoing timer
  var ongoingTimers = Map.empty[Long, Cancellable]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  def receive: Receive = {
    case msg @ Replicate(k, vo, _) =>
      val seq = nextSeq()
      acks = acks updated (seq, (sender, msg))
      ongoingTimers = ongoingTimers updated (seq, context.system.scheduler scheduleWithFixedDelay (
        initialDelay = 0 millis,
        delay = 100 millis,
        receiver = replica,
        message = Snapshot(k, vo, seq)
      ))
    case SnapshotAck(k, seq) =>
      if (ongoingTimers.isDefinedAt(seq) && acks.isDefinedAt(seq)) {
        ongoingTimers(seq).cancel()
        ongoingTimers = ongoingTimers removed seq
        val (recipient, msg) = acks(seq)
        acks = acks removed seq
        recipient ! Replicated(k, msg.id)
      }
  }

}
