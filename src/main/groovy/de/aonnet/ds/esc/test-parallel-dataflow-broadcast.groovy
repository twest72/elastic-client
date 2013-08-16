package de.aonnet.ds.esc

import groovyx.gpars.dataflow.DataflowBroadcast
import groovyx.gpars.dataflow.DataflowReadChannel

/**
 * User: westphal
 * Date: 07.08.13
 * Time: 07:37
 */
DataflowBroadcast broadcastStream = new DataflowBroadcast()
DataflowReadChannel stream1 = broadcastStream.createReadChannel()
DataflowReadChannel stream2 = broadcastStream.createReadChannel()
broadcastStream << 'Message1'
broadcastStream << 'Message2'
broadcastStream << 'Message3'

def val = stream1.val
println val
assert val == stream2.val

val = stream1.val
println val
assert val == stream2.val

val = stream1.val
println val
assert val == stream2.val
