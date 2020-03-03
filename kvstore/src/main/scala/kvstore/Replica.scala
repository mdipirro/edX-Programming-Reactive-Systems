package kvstore

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, Cancellable, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
import kvstore.Arbiter._
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout

import scala.language.postfixOps

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(-1, Duration.Inf) {
    case _: PersistenceException => Restart
  }

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  private val persistence = context.actorOf(Persistence.props(false))
  private var pendingAck = Option.empty[(ActorRef, Cancellable)]

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica(0L))
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(k, v, id) =>
      kv = kv updated (k, v)
      sender ! OperationAck(id)
    case Remove(k, id) =>
      kv = kv removed k
      sender ! OperationAck(id)
    case Get(k, id) => replyToLookup(k, id)
    case _ =>
  }

  def replica(expectedSeq: Long): Receive = {
    case Get(k, id) => replyToLookup(k, id)
    case Insert(_, _, id) => sender ! OperationFailed(id)
    case Remove(_, id) => sender ! OperationFailed(id)
    case Snapshot(k, ov, seq) if seq == expectedSeq =>
      kv = ov.fold(kv removed k)(kv updated (k, _))
      val client = sender

      pendingAck = Some(client -> context.system.scheduler.scheduleWithFixedDelay(
        initialDelay = 0 millis,
        delay = 100 millis,
        receiver = persistence,
        message = Persist(k, ov, seq)
      ))
    case Snapshot(k, ov, seq) if seq < expectedSeq =>
      sender ! SnapshotAck(k, seq) // seq + 1 is certainly <= expectedSeq, so no become() is necessary
    // just ignore the message if seq > expectedSeq
    case Persisted(k, id) if id == expectedSeq => pendingAck foreach { case (client, timer) =>
      pendingAck = None
      context become replica(expectedSeq + 1L)
      timer.cancel()
      client ! SnapshotAck(k, id)
    }
  }

  private def replyToLookup(key: String, opId: Long): Unit = sender ! GetResult(key, kv get key, opId)
}

