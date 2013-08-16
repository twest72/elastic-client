package de.aonnet.ds.esc

import groovyx.gpars.dataflow.stream.DataflowStream
import groovyx.gpars.dataflow.stream.FList
import groovyx.gpars.group.DefaultPGroup
import groovyx.gpars.group.PGroup
import groovyx.gpars.scheduler.ResizeablePool

/**
 * User: westphal
 * Date: 07.08.13
 * Time: 08:40
 */

/**
 * We need a resizeable thread pool, since tasks consume threads while waiting blocked for values at DataflowQueue.val
 */
PGroup group = new DefaultPGroup(new ResizeablePool(true))

int requestedPrimeNumberCount = 500

/**
 * Generating candidate numbers
 */
DataflowStream<Integer> candidates = new DataflowStream<Integer>()
group.task {
    candidates.generate(2, { it + 1 }, { it < 10000 })
}

/**
 * Chain a new filter for a particular prime number to the end of the Sieve
 * @param inChannel The current end channel to consume
 * @param prime The prime number to divide future prime candidates with
 * @return A new channel ending the whole chain
 */
Closure<FList<Integer>> filter = { FList<Integer> inChannel, Integer prime ->
    FList<Integer> f = inChannel.filter { Integer number ->
        group.task {
            return number % prime != 0
        }
    }
    return f
}

/**
 * Consume Sieve output and add additional filters for all found primes
 */
FList<Integer> currentOutput = candidates
requestedPrimeNumberCount.times {
    Integer prime = currentOutput.first
    println "Found: $prime"
    currentOutput = filter(currentOutput, prime)
}
