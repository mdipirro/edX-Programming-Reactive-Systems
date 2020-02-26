/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case msg: Operation => root ! msg
    case _ => ???
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = ???

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
   * Acknowledges that a copy has been completed. This message should be sent
   * from a node to its parent, when this node and all its children nodes have
   * finished being copied.
   */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case msg: Contains => handleContains(msg)
    case msg: Insert => handleInsert(msg)
    case msg: Remove => handleRemove(msg)
    case msg: CopyTo =>
      val children = subtrees.values.toSet
      if (removed && children.isEmpty) {
        terminateAndNotifyCopyDone()
      } else {
        if (!removed) {
          msg.treeNode ! Insert(self, 0, elem)
        }
        context become copying(children, removed)
        children foreach(_ ! msg)
      }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished =>
      if (expected.isEmpty) terminateAndNotifyCopyDone()
      else copying(expected, insertConfirmed = true)
    case CopyFinished =>
      val newExpected = expected - sender
      if (newExpected.isEmpty && insertConfirmed) terminateAndNotifyCopyDone()
      else copying(newExpected, insertConfirmed)
  }

  private def handleContains(msg: Contains): Unit = {
    def replyWith(value: Boolean): Unit = msg.requester ! ContainsResult(msg.id, result = value)

    handleMessage(
      msg = msg,
      ifEqual = replyWith(!removed),
      ifEmptyLeft = replyWith(false),
      ifEmptyRight = replyWith(false)
    )
  }

  private def handleInsert(msg: Insert): Unit = {
    def insertInPosition(pos: Position): Unit = {
      subtrees += (pos -> context.actorOf(BinaryTreeNode.props(msg.elem, initiallyRemoved = false), s"node($elem)"))
      msg.requester ! OperationFinished(msg.id)
    }

    handleMessage(
      msg = msg,
      ifEqual = {
        if (removed) removed = false
        msg.requester ! OperationFinished(msg.id)
      },
      ifEmptyLeft = insertInPosition(Left),
      ifEmptyRight = insertInPosition(Right)
    )
  }

  private def handleRemove(msg: Remove): Unit = {
    def reply(): Unit = msg.requester ! OperationFinished(msg.id)

    handleMessage(
      msg = msg,
      ifEqual = {
        removed = true
        reply()
      },
      ifEmptyLeft = reply(),
      ifEmptyRight = reply()
    )
  }

  private def handleMessage(
                             msg: Operation,
                             ifEqual: => Unit,
                             ifEmptyLeft : => Unit,
                             ifEmptyRight: => Unit
                           ): Unit =
    msg.elem match {
      case e if e == elem => ifEqual
      case e if e < elem && !subtrees.isDefinedAt(Left) => ifEmptyLeft
      case e if e < elem => subtrees(Left) ! msg
      case e if e > elem && !subtrees.isDefinedAt(Right) => ifEmptyRight
      case e if e > elem => subtrees(Right) ! msg
    }

  private def terminateAndNotifyCopyDone(): Unit = {
    context.parent ! CopyFinished
    context stop self
  }
}
