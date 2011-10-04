package akka.dispatch

import java.util.concurrent._
import java.util.concurrent.atomic._

class Benchmarker {
  private case class Benchmark(iterations : Int, itemsPerIteration : Int, name : String, body : () => Unit)
  private case class BenchmarkResult(name : String, timeNanosPerItem : Long)

  private var benchmarks : List[Benchmark] = List()
  private var results : List[BenchmarkResult] = List()

  private def recordIteration(name : String, itemsPerIteration : Int, body : () => Unit) {
    val start = System.nanoTime()
    body()
    val end = System.nanoTime()
    val nanosPerIteration = (end - start) / itemsPerIteration.toDouble
    results = BenchmarkResult(name, nanosPerIteration.toLong) :: results
  }

  def addBenchmark(iterations : Int, itemsPerIteration : Int, name : String, body : () => Unit) {
    benchmarks = Benchmark(iterations, itemsPerIteration, name, body) :: benchmarks
  }

  case class GCInfo(collections: Long, time: Long, used: Long)

  private def gcInfo(): GCInfo = {
    import scala.collection.JavaConverters._

    var totalGarbageCollections = 0L
    var garbageCollectionTime = 0L

    for (gc <- java.lang.management.ManagementFactory.getGarbageCollectorMXBeans().asScala) {
      val count = gc.getCollectionCount
      val time = gc.getCollectionTime
      if (count > 0)
        totalGarbageCollections += count
      if (time > 0)
        garbageCollectionTime += time
    }
    GCInfo(totalGarbageCollections, garbageCollectionTime,
           Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())
  }

  private def warmup(iterations : Int, name : String, body : () => Unit, gcStats: Boolean = false) {
    System.gc()
    val initialGC = gcInfo()
    var peakUsedMem = 0L
    val tenth = if (iterations >= 10) iterations / 10 else 1
    printf("  warming up '%s' ", name)
    System.out.flush()
    for (i <- 1 to iterations) {
      if ((i % tenth) == 0) {
        printf(".")
        System.out.flush()
        val usedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()
        peakUsedMem = math.max(peakUsedMem, usedMem)
      }
      body()
    }
    printf("done\n")
    val afterGC = gcInfo()
    System.gc()
    val afterAnotherGC = gcInfo()
    val peakIncrease = (math.max(peakUsedMem, afterGC.used) - initialGC.used) / 1024.0
    val increased = (afterGC.used - initialGC.used) / 1024.0
    val recovered = (afterGC.used - afterAnotherGC.used) / 1024.0
    if (gcStats) {
      printf("    bogus memory stats:\n")
      printf("        %.0fK peak mem increase %.0fK ending mem increase, %.0fK recovered with a GC, %.0fK final change\n",
             peakIncrease, increased, recovered, (afterAnotherGC.used - initialGC.used) / 1024.0)
      printf("        %d GCs during the %d-iterations warmup using %dms\n",
             afterGC.collections - initialGC.collections, iterations,
             afterGC.time - initialGC.time)
    }
  }

  private def run(iterations : Int, itemsPerIteration : Int, name : String, body : () => Unit) : Unit = {
    val tenth = if (iterations >= 10) iterations / 10 else 1

    printf("  running '%s' ", name)
    System.out.flush()
    System.gc() // encourage GC to happen outside of our timed portion
    for (i <- 1 to iterations) {
      if ((i % tenth) == 0) {
        printf(".")
        System.out.flush()
        System.gc()
      }
      recordIteration(name, itemsPerIteration, body)
    }
    printf("done\n")
  }

  def runAll() : Unit = {
    benchmarks = benchmarks.sortBy({ _.name })
    for (b <- benchmarks) {
      warmup((b.iterations * 1.5).toInt, b.name, b.body, gcStats = true)
    }
    for (b <- benchmarks) {
      // warmup again a little bit, in case there's some effect
      // caused by doing other stuff since the first warmup
      warmup(b.iterations / 2, b.name, b.body)
      run(b.iterations, b.itemsPerIteration, b.name, b.body)
    }
  }

  def output() : Unit = {
    val splitByName = results.foldLeft(Map.empty[String, List[BenchmarkResult]])({ (sofar, result) =>
          val prev = sofar.getOrElse(result.name, Nil)
          sofar + Pair(result.name, result :: prev)
        })
    val sortedByName = splitByName.iterator.toSeq.sortBy(_._1)

    for ((name, resultList) <- sortedByName) {
      val iterations = resultList.length

      val sortedTimes = resultList.map({ _.timeNanosPerItem }).toSeq.sorted

      /* First find the median */
      val (firstHalf, secondHalf) = sortedTimes splitAt (iterations / 2)
      val median = if (iterations % 2 == 0) {
        (firstHalf.last + secondHalf.head) / 2
      } else {
        secondHalf.head
      }

      /* Second find a trimmed mean */
      val toTrim = iterations / 8
      val trimmedTimes = sortedTimes.drop(toTrim).dropRight(toTrim)
      val numberAfterTrim = sortedTimes.length - toTrim * 2
      require(trimmedTimes.length == numberAfterTrim)
      val totalTrimmedTimes = trimmedTimes.reduceLeft(_ + _)

      val untrimmedAverageTime = sortedTimes.reduceLeft(_ + _).toDouble / iterations
      val trimmedAverageTime = totalTrimmedTimes.toDouble / numberAfterTrim

      val untrimmedMillisPerIteration = untrimmedAverageTime / TimeUnit.MILLISECONDS.toNanos(1)
      val trimmedMillisPerIteration = trimmedAverageTime / TimeUnit.MILLISECONDS.toNanos(1)
      val medianMillisPerIteration = median.toDouble / TimeUnit.MILLISECONDS.toNanos(1)

      printf("  %-35s %15.3f ms/item trimmed avg (%.3f median %.3f untrimmed %.3f min) over %d iterations\n",
             name, trimmedMillisPerIteration, medianMillisPerIteration, untrimmedMillisPerIteration,
             sortedTimes.head.toDouble / TimeUnit.MILLISECONDS.toNanos(1),
             iterations)
    }
  }
}

class ExpiresImmediately(val latch: CountDownLatch) extends Expirable {
  val serial = ExpiresImmediately.serial.getAndIncrement

  @volatile var expired = false
  override def checkExpired(currentTimeNanos: Long): Long = {
    if (!expired) {
      expired = true
      latch.countDown
    }
    0
  }

  override def toString = {
    "ExpiresImmediately(%d,expired=%b)".format(serial, expired)
  }
}

object ExpiresImmediately {
  val serial = new AtomicInteger(0)
}

// this is the pseudo-realistic case
class ExpiresVarying(val latch: CountDownLatch) extends Expirable {
  val serial = ExpiresVarying.serial.getAndIncrement

  val startTime = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis)
  // completion varies from 0 to 90ms after creation
  val completeTime = startTime + TimeUnit.MILLISECONDS.toNanos((ExpiresVarying.cycle.getAndIncrement % 10) * 10)
  // "expires" in 75ms, so usually after completion
  val expireTime = startTime + TimeUnit.MILLISECONDS.toNanos(75)
  @volatile var expired = false

  override def checkExpired(currentTimeNanos: Long): Long = {
    val left = expireTime - currentTimeNanos
    if (left > 0) {
      if (completeTime <= currentTimeNanos) {
        if (!expired) {
          expired = true
          //println("completing, remaining " + latch.getCount)
          latch.countDown
        }
        0 // "completed"
      } else {
        left
      }
    } else {
      if (!expired) {
        expired = true
        //println("expiring, remaining    " + latch.getCount)
        latch.countDown
      }
      0
    }
  }

  override def toString = {
    "ExpiresVarying(%d,expired=%b)".format(serial, expired)
  }
}

object ExpiresVarying {
  val cycle = new AtomicInteger(0)
  val serial = new AtomicInteger(0)
}

object Benchmark extends App {
  benchmarkExpiresImmediately()
  benchmarkExpiresVarying()

  def fromManyThreads(threads: Int, body: () => Unit) {
    // enough threads to get some contention
    val executor = Executors.newFixedThreadPool(threads)
    val runnable = new Runnable() {
      override def run() {
        try {
          body()
        } catch {
          case e => {
            System.err.println("" + e.getClass.getSimpleName + ": " + e.getMessage)
            System.err.println(e.getStackTraceString)
            System.exit(1)
          }
        }
      }
    }
    for (i <- 1 to threads) {
      executor.execute(runnable)
    }
    executor.shutdown()
    executor.awaitTermination(120, TimeUnit.SECONDS)
  }

  def benchmarkExpiresImmediately() {
    val benchmarker = new Benchmarker

    val iterations = 500
    val itemsToExpire = 100000
    val threads = 40 // should divide evenly into itemsToExpire
    assert((itemsToExpire % threads) == 0)

    def doExpireImmediately(service: ExpirationService) {
      val latch = new CountDownLatch(itemsToExpire)
      fromManyThreads(threads, { () =>
        for (i <- 1 to (itemsToExpire / threads)) {
          service.add(new ExpiresImmediately(latch))
        }
      })
      service.flush()
      latch.await
    }

    benchmarker.addBenchmark(iterations, 1, "ConcurrentLinkedQueue", () => {
      val service = new ConcurrentQueueExpirationService
      doExpireImmediately(service)
      service.shutdown()
    })

    // this one is one-tenth the iterations because it's so miserably slow
    benchmarker.addBenchmark(iterations / 10, 1, "scheduleOnce per expirable", () => {
      val service = new NaiveImmediateExpirationService
      doExpireImmediately(service)
      service.shutdown()
    })

    for (batch <- Seq(100, 1000, 5000)) {
      benchmarker.addBenchmark(iterations, 1, "BatchDrained maxBatch=%4d".format(batch),
                               () => {
                                 val service = new BatchDrainedExpirationService(maxBatch = batch)
                                 doExpireImmediately(service)
                                 service.shutdown()
                               })
    }

    benchmarker.runAll()

    println("Results")
    println("=======")
    println("Benchmarking: immediate expiration, so measuring overhead in case we never schedule an expiration task.")
    println("One 'item' is %d expirations performed by %d threads concurrently".format(itemsToExpire, threads))
    benchmarker.output()
    println("=======")
    println("")
  }

  def benchmarkExpiresVarying() {
    val benchmarker = new Benchmarker

    val iterations = 10
    val itemsToExpire = 100000
    val threads = 40 // should divide evenly into itemsToExpire
    assert((itemsToExpire % threads) == 0)

    def doExpireVarying(service: ExpirationService) {
      val latch = new CountDownLatch(itemsToExpire)
      fromManyThreads(threads, { () =>
        for (i <- 1 to (itemsToExpire / threads)) {
          service.add(new ExpiresVarying(latch))
        }
      })
      latch.await
    }

    benchmarker.addBenchmark(iterations, 1, "scheduleOnce per expirable", () => {
      val service = new NaiveExpirationService
      doExpireVarying(service)
      service.shutdown()
    })

    for (resolutionMs <- Seq(1, 5, 21, 200)) {
      val resolutionNs = TimeUnit.MILLISECONDS.toNanos(resolutionMs)

      benchmarker.addBenchmark(iterations, 1, "ConcurrentLinked      res=%4dms".format(resolutionMs),
                               () => {
                                 val service = new ConcurrentQueueExpirationService(resolutionNs)
                                 doExpireVarying(service)
                                 service.shutdown()
                               })


      for (batch <- Seq(100, 3000)) {
        benchmarker.addBenchmark(iterations, 1, "BatchDrained bat=%4d res=%4dms".format(batch, resolutionMs),
                                 () => {
                                   val service = new BatchDrainedExpirationService(maxBatch = batch,
                                                      resolutionInNanos = resolutionNs)
                                   doExpireVarying(service)
                                   service.shutdown()
                                 })
      }
    }

    benchmarker.runAll()

    println("Results")
    println("=======")
    println("Benchmarking: 'realistic' expiration, showing effect of resolution.")
    println("   (one non-realism: uniform rather than normal distribution of completion)")
    println("   (in real life most likely more completions happen earlier)")
    println("   (the fake expirables have a 75ms timeout with varying completion time)")
    println("   (so the best theoretically achievable benchmark would be 75ms, if we had res=0ms)")
    println("One 'item' is %d expirations performed by %d threads concurrently".format(itemsToExpire, threads))
    benchmarker.output()
    println("=======")
    println("")
  }
}

