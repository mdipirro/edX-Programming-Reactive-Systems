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

  private case class OperationTimeout(replyTo: ActorRef, id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  override def preStart(): Unit = arbiter ! Join

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(-1, Duration.Inf) {
    case _: PersistenceException => Restart
  }

  private case class UnackedOperation(ackTo: ActorRef,
                                      persistenceTimer: Option[Cancellable],
                                      operationTimer: Option[Cancellable],
                                      waitingForReplicators: Set[ActorRef]
                                   )

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  private val persistence = context.actorOf(persistenceProps)
  private var acks = Map.empty[Long, UnackedOperation]

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica(0L))
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(k, v, id) =>
      kv = kv updated (k, v)
      persistAndReplicate(k, Some(v), id, 100 millis)

    case Remove(k, id) =>
      kv = kv removed k
      persistAndReplicate(k, None, id, 100 millis)

    case Get(k, id) => replyToLookup(k, id)

    case Persisted(_, id) =>
      updateStatus(id, {ack =>
        ack.persistenceTimer foreach (_.cancel())
        ack.copy(persistenceTimer = None)
      })
      if (acked(id)) {
        println("PrimaryPersisted")
        cleanUpAndAck(id, _ ! OperationAck(id))
      }

    case Replicated(_, id) =>
      updateStatus(id, ack => ack.copy(waitingForReplicators = ack.waitingForReplicators - sender))
      if (acked(id)) {
        println("PrimaryReplicated")
        cleanUpAndAck(id, _ ! OperationAck(id))
      }

    case OperationTimeout(client, id) =>
      println("OperationTimeout")
      cleanUpAndAck(id, _ => client ! OperationFailed(id))
  }

  def replica(expectedSeq: Long): Receive = {
    case Get(k, id) => replyToLookup(k, id)
    case Insert(_, _, id) => sender ! OperationFailed(id)
    case Remove(_, id) => sender ! OperationFailed(id)

    case Snapshot(k, ov, seq) if seq == expectedSeq =>
      kv = ov.fold(kv removed k)(kv updated (k, _))
      acks = acks updated (seq, UnackedOperation(
        ackTo = sender,
        persistenceTimer = Some(retryPersistence(k, ov, seq, 100 millis)),
        operationTimer = None,
        waitingForReplicators = Set.empty
      ))

    case Snapshot(k, _, seq) if seq < expectedSeq =>
      sender ! SnapshotAck(k, seq) // seq + 1 is certainly <= expectedSeq, so no become() is necessary
    // just ignore the message if seq > expectedSeq

    case Persisted(k, id) if id == expectedSeq =>
      println("SecondaryPersisted")
      cleanUpAndAck(id, {
        context become replica(expectedSeq + 1L)
        _ ! SnapshotAck(k, id)
      })
  }

  private def replyToLookup(key: String, opId: Long): Unit = sender ! GetResult(key, kv get key, opId)

  private def persistAndReplicate(key: String, value: Option[String], opId: Long, retryEvery: FiniteDuration): Unit = {
    val client = sender
    val operationTimer = context.system.scheduler.scheduleOnce(1 second, self, OperationTimeout(client, opId))
    acks = acks updated (opId, UnackedOperation(
      ackTo = client,
      persistenceTimer = Some(retryPersistence(key, value, opId, retryEvery)),
      operationTimer = Some(operationTimer),
      waitingForReplicators = replicators
    ))
    replicators foreach (_ ! Replicate(key, value, opId))

  }

  private def retryPersistence(key: String, value: Option[String], opId: Long, interval: FiniteDuration): Cancellable =
    context.system.scheduler.scheduleWithFixedDelay(
      initialDelay = 0 millis,
      delay = interval,
      receiver = persistence,
      message = Persist(key, value, opId)
    )

  private def updateStatus(id: Long, updateWith: UnackedOperation => UnackedOperation): Unit =
    acks get id foreach {ack => acks = acks updated (id, updateWith(ack))}

  private def acked(id: Long) =
    (acks get id).fold(false)(ack => ack.persistenceTimer.isEmpty && ack.waitingForReplicators.isEmpty)

  private def cleanUpAndAck(opId: Long, ackTo: ActorRef => Unit): Unit = {
    acks get opId foreach { case UnackedOperation(recipient, persistenceTimer, operationTimer, _) =>
      List(persistenceTimer, operationTimer) foreach (_ foreach (_.cancel()))
      acks = acks removed opId
      ackTo(recipient)
    }
  }
}

