package de.aonnet.ds.esc

import groovyx.gpars.dataflow.DataflowQueue

import static groovyx.gpars.dataflow.Dataflow.task

/**
 * User: westphal
 * Date: 07.08.13
 * Time: 07:37
 */
List<String> words = ['Groovy', 'fantastic', 'concurrency', 'fun', 'enjoy', 'safe', 'GPars', 'data', 'flow']
DataflowQueue buffer = new DataflowQueue()

task {
    println 'start task'
    words.each { String word ->
        buffer << word.toUpperCase()  //add to the buffer
        Thread.sleep(200)
    }
    println 'end task'
}

// wait for buffer
while (buffer.length() <= 0) {
    println 'wait for buffer'
    Thread.sleep(100)
}

//read from the buffer in a loop
println 'start out'
while (buffer.length() > 0) {
    String out = buffer.val
    println "buffer length: ${buffer.length()} out: $out"
    if (buffer.length() <= 0) {
        println 'wait for buffer'
        Thread.sleep(400)
    }
}
println 'end out'
