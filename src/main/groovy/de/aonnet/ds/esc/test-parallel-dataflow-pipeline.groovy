package de.aonnet.ds.esc

import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowReadChannel

import static groovyx.gpars.dataflow.Dataflow.splitter

/**
 * User: westphal
 * Date: 07.08.13
 * Time: 08:40
 */

def toUpperCase = { s ->
    s.toUpperCase()
}
def save = { text ->
    //Just pretending to be saving the text to disk, database or whatever
    println 'Saving ' + text
}
final toEncrypt = new DataflowQueue()
final DataflowReadChannel encrypted = toEncrypt.chainWith toUpperCase chainWith { it.reverse() } chainWith { '###encrypted###' + it + '###' }


final DataflowQueue fork1 = new DataflowQueue()
final DataflowQueue fork2 = new DataflowQueue()
splitter(encrypted, [fork1, fork2])  //Split the data flow

fork1.chainWith save  //Hook in the save operation

//Hook in a sneaky decryption pipeline
final DataflowReadChannel decrypted = fork2.chainWith {
    it[15..-4]
}.chainWith {
    it.reverse()
}.chainWith {
    it.toLowerCase()
}.chainWith {
    'Groovy leaks! Check out a decrypted secret message: ' + it
}

toEncrypt << "I need to keep this message secret!"
toEncrypt << "GPars can build operator pipelines really easy"

println decrypted.val
println decrypted.val
