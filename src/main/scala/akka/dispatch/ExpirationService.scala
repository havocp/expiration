/**
 *  Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.dispatch

import scala.annotation.tailrec
import scala.collection.immutable.Queue

import java.util.concurrent._
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.atomic.AtomicInteger
import akka.actor._

/**
 * An object which expires at some point, but is likely
 * to be completed well before that point, so we can often
 * stop tracking the expiration early. Such as a Future.
 * This is done as a trait (rather than a dedicated task
 * object as in ScheduledExecutorService) to minimize
 * overhead.
 */
private[akka] trait Expirable {
  /**
   * Returns the time remaining in nanoseconds. If zero or negative, then
   * the expirable can consider itself expired, and check will never be
   * called again. If positive, the check will be called an undefined number
   * of times (but at least once) during the time remaining.
   *
   * This must run quickly, since we don't update currentTimeNanos
   * as we iterate over a bunch of expirables.
   *
   * This method doubles as notification; if an expirable decides
   * it has <= 0 remaining, it has to self-expire.
   */
  def checkExpired(currentTimeNanos: Long): Long
}

/**
 * A service that tracks and expires expirables.
 * It tries to minimize overhead by bunching
 * up expirables with a nearby expiration time,
 * leaving the expiration time a little imprecise.
 * Trying to avoid adding a bunch of
 * tasks to the Scheduler. This also avoids creating
 * the FutureTask objects the scheduler creates and
 * otherwise avoids the scheduler's overhead.
 * Basically we want to make the common case where
 * a future is quickly completed really fast.
 *
 * ExpirationService doesn't care about errors lower than
 * its resolution. Larger errors are still
 * possible due to OS thread scheduling and OS timer resolution
 * and so forth, but we don't even try for errors below
 * the requested resolution. Too-low resolution will lead
 * to CPU and memory cost.
 */
private[akka] trait ExpirationService {
  // add() does not immediately checkExpired;
  // it assumes that you just did that, and know
  // the expirable is not yet expired.
  def add(expirable: Expirable): Unit

  // flush() is only useful for benchmarking; it's supposed to immediately
  // checkExpired all the expirables. This keeps us from
  // waiting the actual timeout so we can measure pure overhead.
  def flush(): Unit

  def addMany(expirables: Seq[Expirable]) {
    for (e <- expirables)
      add(e)
  }

  def shutdown() {
  }
}

// this is completely bogus for real-world usage; the issue is that
// to implement the benchmark-only method flush(), we'd have to
// track all the ScheduledFuture handles. To avoid that we
// just assume flush() will be called and schedule for 1ns.
private[akka] class NaiveImmediateExpirationService extends ExpirationService {
  override def add(expirable: Expirable) {
    Scheduler.scheduleOnce({ () =>
      if (expirable.checkExpired(TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis)) > 0)
        throw new IllegalStateException("this expiration service doesn't work for non-already-expired expirables")
    }, 1, TimeUnit.NANOSECONDS)
  }
  override def flush() {
    // no-op because we didn't keep ScheduledFuture
  }
}

// a service that schedules a new Scheduler task for every expirable,
// for that expirable's actual timeout
private[akka] class NaiveExpirationService extends ExpirationService {
  override def add(expirable: Expirable) {
    val expires = expirable.checkExpired(TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis))
    if (expires > 0) {
      Scheduler.scheduleOnce({ () =>
        // check again, re-add if we didn't expire yet
        add(expirable)
      },
                             expires + 10, // + 10ns to reduce risk of gratuitous extra scheduler task
                             TimeUnit.NANOSECONDS)
    }
  }

  override def flush() {
    throw new UnsupportedOperationException("don't use this with flush")
  }
}

private[akka] abstract class BatchingExpirationService(resolutionInNanos: Long) extends ExpirationService {
  def queue(expirable: Expirable): Unit

  def queueMany(expirables: Seq[Expirable]): Unit

  final override def add(expirable: Expirable) {
    queue(expirable)
    scheduleBatch
  }

  final override def addMany(expirables: Seq[Expirable]) {
    queueMany(expirables)
    scheduleBatch
  }

  final override def flush() {
    clearBatch.run()
  }

  @volatile
  private var batchScheduled = false

  final private def scheduleBatch {
    // could use AtomicReferenceFieldUpdater perhaps for extra speed?
    // FIXME ideally this is very fast, so we should do something without
    // the explicit synchronized. trying double-checked for now.
    if (!batchScheduled) { // safe?
      synchronized {
        if (!batchScheduled) {
          Scheduler.scheduleOnce(clearBatch, resolutionInNanos, TimeUnit.NANOSECONDS)
          batchScheduled = true
        }
      }
    }
  }

  // using Iterator or something would be prettier than this "next"
  // function that returns null, but this is 20% faster on our
  // microbenchmark
  final protected def recheck(next: => Expirable) {
    var buckets = Map.empty[Long, List[Expirable]]

    val now = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis) // get this once for the whole batch
    var e = next
    while (e != null) {
      val remaining = e.checkExpired(now)
      if (remaining > 0) {
        // bucket the expirables by rounded-off expiration time, be careful
        // to round up so we don't get a time of 0
        val bucket = (remaining / resolutionInNanos) + resolutionInNanos
        //assert(bucket > 0)
        //assert((bucket % resolutionInNanos) == 0)
        buckets.get(bucket) match {
          case Some(old) ⇒
          buckets += (bucket -> (e :: old))
          case None ⇒
          buckets += (bucket -> (e :: Nil))
        }
      } else {
        // nothing more to do, the expirable is expired.
      }
      e = next
    }

    for ((remaining, expirables) ← buckets) {
      // for each bucket, re-add the expirables in the bucket.
      // note that this leads to again grouping expirables within
      // another initial completion window, which may put tasks
      // together that were originally in a different window.
      Scheduler.scheduleOnce({ () ⇒
                              addMany(expirables)
                            }, remaining, TimeUnit.NANOSECONDS)
    }
  }

  protected def drainNow(): Unit

  final private val clearBatch = new Runnable {
    override def run() = {
      synchronized {
        batchScheduled = false
        // after this point another clearBatch could
        // schedule and run, which means two of them
        // might drain the queue at once in theory,
        // but that should be fine even if it happens
        // since it's a concurrent queue.
      }

      drainNow()
    }
  }
}

object BatchingExpirationService {
  val defaultResolution = TimeUnit.MILLISECONDS.toNanos(21)
}

private[akka] class ConcurrentQueueExpirationService(resolutionInNanos: Long = BatchingExpirationService.defaultResolution) extends BatchingExpirationService(resolutionInNanos) {

  // queue of expirables that we batch up to schedule all at once. In
  // particular, in the hopefully common case that many futures are
  // completed within a few milliseconds, we would schedule a single
  // timer for all of them.
  private val batchQueue = new ConcurrentLinkedQueue[Expirable]

  override def queue(expirable: Expirable) {
    batchQueue.add(expirable)
  }

  override def queueMany(expirables: Seq[Expirable]) {
    for (e <- expirables)
      batchQueue.add(e) // FIXME addAll?
  }

  override def drainNow() {
    recheck({ batchQueue.poll })
  }
}

private[akka] class BatchDrainedExpirationService(maxBatch: Int, resolutionInNanos: Long = BatchingExpirationService.defaultResolution) extends BatchingExpirationService(resolutionInNanos) {

  private val drainer = new BatchDrainer[Expirable]() {
    override def drain(batch: Array[Expirable], count: Int) {
      var i = -1
      def next = {
        i = i + 1
        if (i == count) {
          null
        } else {
          batch(i)
        }
      }
      recheck(next)
    }
  }

  private val drain = new BatchDrain[Expirable](maxBatch, drainer)

  override def queue(expirable: Expirable) {
    drain.add(expirable)
  }

  override def queueMany(expirables: Seq[Expirable]) {
    for (e <- expirables)
      drain.add(e)
  }

  override def drainNow() {
    drain.flush()
  }
}

private[akka] trait BatchDrainer[T <: AnyRef] {
  // "drain" MUST only touch the array while it's on the stack;
  // if it wants to defer handling, it has to copy the array.
  // i.e. the drain must be synchronous.
  def drain(batch: Array[T], count: Int): Unit
}

private class Batch[T <: AnyRef : Manifest](val maxSize: Int, val slots: Array[T], val recycleTo: BlockingQueue[Batch[T]]) {
  require(slots.size == maxSize)
  val reserved = new AtomicInteger(maxSize)
  val filled = new AtomicInteger(maxSize)
  // padding = number of slots that don't have real data.
  // "padding" can only change once (set by the one thread that does a flush() on the batch)
  // and "padding" is only read while completing the batch, which is done by one thread
  // and done sequentially after the flush() writes to it.
  @volatile
  var padding = 0

  // DEBUG HACK ONLY
  @volatile
  var maybeRecycled = true

  // DEBUG ONLY
  @volatile
  var whyRecycled: String = "initial construct"

  def recycle() {
    // these have to continue to prevent any thread that
    // tests them while we're dead from trying to use the
    // recycled batch.
    assert(reserved.get >= maxSize)
    assert(filled.get == maxSize)
    padding = 0
    whyRecycled = "in the dead pool, was: " + whyRecycled
    recycleTo.add(this)
  }

  def unrecycle() {
    try {
      assert(maybeRecycled)
      assert(filled.get == maxSize)
      assert(reserved.get >= maxSize)
      assert(padding == 0)
    } catch {
      case e => {
        println("unrecycle broken: " + this)
        throw e
      }
    }

    // it's possible a thread still has hold of the batch
    // from when it was previously current. having reserved
    // >= maxSize was keeping that thread from doing anything
    // with it; so right here, we enable that thread to again
    // touch the object. we set other stuff first, then reserved,
    // because reserved < maxSize is the thing that lets a thread
    // get a reservation and thus start using the batch.
    whyRecycled = "not recycled, alive again"
    maybeRecycled = false
    filled.set(0)
    reserved.set(0) // do this LAST to "unlock"
  }

  override def toString = {
    "Batch(reserved=%d,filled=%d,padding=%d)".format(reserved.get, filled.get, padding) + " whyRecycled=" + whyRecycled
  }
}

private class BatchPool[T <: AnyRef : Manifest](poolSize: Int,
                                                maxBatch: Int,
                                                synchronousDrainer: BatchDrainer[T]) {
  // FIXME it would be better for cache behavior if this were a LIFO
  // rather than a FIFO.
  private val batchPool = new ArrayBlockingQueue[Batch[T]](poolSize)

  private final def createBatch() = {
    new Batch[T](maxBatch, slots = new Array[T](maxBatch), recycleTo=batchPool)
  }

  for (i <- 1 to poolSize) {
    val b = createBatch()
    b.maybeRecycled = true
    b.whyRecycled = "initial creation"
    batchPool.put(b)
  }

  final def repeatIfInterrupted[T](body: => T): T = {
    try {
      body
    } catch {
      case e: InterruptedException =>
        // this is unfortunately not a tail call, not sure I understand why
        repeatIfInterrupted(body)
    }
  }

  def take(): Batch[T] = {
    val newBatch = repeatIfInterrupted({ batchPool.take() })
    assert(newBatch != null)

    newBatch.unrecycle()

    newBatch
  }

  def complete(completed: Batch[T]) {
    assert(!completed.maybeRecycled)
    completed.maybeRecycled = true

    assert(completed.filled.get == completed.maxSize)
    assert(completed.reserved.get >= completed.maxSize)

    // drain the batch
    synchronousDrainer.drain(completed.slots, completed.maxSize - completed.padding)

    // now recycle
    completed.recycle()
  }
}

/* This does NOT guarantee FIFO, i.e. it's not a queue.
 * It does however reliably send everything you give it
 * to the drainer you give it.
 */
private[akka] class BatchDrain[T <: AnyRef : Manifest](maxBatch: Int,
                                                       synchronousDrainer: BatchDrainer[T]) {

  // drainCount ends up meaning the number of threads that can run synchronousDrainer at once,
  // since each UnreliableBatchDrain runs the drainer in the calling thread.
  val drainCount = Runtime.getRuntime().availableProcessors()
  val drains = new Array[UnreliableBatchDrain[T]](drainCount)

  // two batches per drain, so each drain can have one live and one spare
  private val batchPoolSize = drainCount * 2
  private val batchPool = new BatchPool[T](batchPoolSize, maxBatch, synchronousDrainer)

  for (i <- 0 until drainCount) {
    drains(i) = new UnreliableBatchDrain[T](batchPool)
  }

  // the general idea is to stay with the same drain until
  // it won't take our items, and then switch to another one
  // and stick to that one.
  val current = new AtomicInteger(0)

  @tailrec
  final def add(t: T) {
    val c = current.get
    if (drains(c).offer(t)) {
      // ok! best case scenario.
    } else {
      // suck! we need a new drain
      if (c == (drainCount - 1)) {
        current.compareAndSet(c, 0)
      } else {
        current.compareAndSet(c, c + 1)
      }
      // now start over.
      add(t)
    }
  }

  def flush() {
    drains.foreach(_.flush())
  }
}

/* This does NOT guarantee FIFO, i.e. it's not a queue;
 * it's also allowed to fail to queue stuff (returning false from offer())
 */
private[akka] class UnreliableBatchDrain[T <: AnyRef : Manifest](batchPool: BatchPool[T]) {

  @volatile
  private var currentBatch = batchPool.take()
  currentBatch.whyRecycled = "initial current batch"

  // the general goal is to make an this just an array assignment
  // and a couple of integer increments.
  final def offer(t: T): Boolean = {
    val current = currentBatch

    // the "lock" on a Batch is owning a reserved slot
    // less than maxSize, without having incremented
    // "filled". i.e. if you increment reserved,
    // get a valid array index, then the Batch is
    // "locked" by you until you increment "filled"
    // Also, whichever thread gets the final slot
    // index has the lock/responsibility to complete
    // the batch (complete = drain and recycle)

    val reserved = current.reserved.getAndIncrement
    if (reserved >= current.maxSize) {
      // "current" can now be recycled! because
      // we have no valid reservation blocking it.
      // maxSize is ideally large enough that
      // this rarely happens.
      // But we have to bail out here.
      false
    } else {
      try {
        assert(current.filled.get < current.maxSize)
        assert(!current.maybeRecycled)
      } catch {
        case e => {
          println("slots=" + current.slots.toSeq)
          println("reserved=" + reserved)
          println("filled=" + current.filled.get)
          println("maxSize=" + current.maxSize)
          println("whyRecycled=" + current.whyRecycled)
          System.out.flush()
          throw e
        }
      }
      current.slots(reserved) = t
      // once we increment "filled" the batch can be recycled,
      // so don't touch the mutable parts anymore.
      val filled = current.filled.incrementAndGet
      assert(filled <= current.maxSize)
      if (filled == current.maxSize) {
        assert(!current.maybeRecycled)
        // we are the last add() so swap out the current batch and drain it
        current.whyRecycled = "final slot filled with an item"
        completeBatch(current)
      }
      true
    }
  }

  private final def completeBatch(completed: Batch[T]) {
    currentBatch = batchPool.take()
    batchPool.complete(completed)
  }

  @tailrec
  final def flush() {
    val current = currentBatch

    if (current.reserved.get > 0) {
      // fill up current batch immediately then drain it
      val current = currentBatch

      val reserved = current.reserved.get
      if (reserved < current.maxSize) {
        if (current.reserved.compareAndSet(reserved, current.maxSize)) {
          val delta = current.maxSize - reserved

          // mark that the newly-reserved slots are just padding
          assert(current.padding == 0)
          current.padding = delta

          // and once we increment filled, the batch's slots
          // can get recycled...
          val filled = current.filled.addAndGet(delta)

          // other threads may not have filled their slots yet,
          // the last one to do so will complete the batch,
          // or if we're the last, we complete it.
          if (filled == current.maxSize) {
            assert(!current.maybeRecycled)
            current.whyRecycled = "added " + current.padding + " padding"
            completeBatch(current)
          }
        } else {
          // failed to get reservation, start over
          flush()
        }
      }
    }
  }
}
