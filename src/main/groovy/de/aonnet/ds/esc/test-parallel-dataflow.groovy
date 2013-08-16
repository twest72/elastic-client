package de.aonnet.ds.esc

import groovyx.gpars.dataflow.DataflowVariable
import groovyx.gpars.dataflow.Dataflows

import static groovyx.gpars.dataflow.Dataflow.task

/**
 * User: westphal
 * Date: 07.08.13
 * Time: 08:40
 */

// In brief, you generally perform three operations with Dataflow variables:
//
// Create a dataflow variable
// Wait for the variable to be bound (read it)
// Bind the variable (write to it)
// And these are the three essential rules your programs have to follow:
//
// When the program encounters an unbound variable it waits for a value.
// It is not possible to change the value of a dataflow variable once it is bound.
// Dataflow variables makes it easy to create concurrent stream agents.

// with dataflows
def df = new Dataflows()

task {
    println 'sum start'
    df.z = df.x + df.y
    println 'sum end'
}

task {
    println 'x start'
    df.x = 10
    println 'x end'
}

task {
    println 'y start'
    df.y = 5
    println 'y end'
}

println "Result: ${df.z}"

// with dataflow variables
def x = new DataflowVariable()
def y = new DataflowVariable()
def z = new DataflowVariable()

task {
    println 'sum start'
    z << x.val + y.val
    println 'sum end'
}

task {
    println 'x start'
    x << 10
    println 'x end'
}

task {
    println 'y start'
    y << 5
    println 'y end'
}

println "Result: ${z.val}"
