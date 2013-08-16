package de.aonnet.ds.esc

import groovyx.gpars.dataflow.DataflowVariable

import static groovyx.gpars.GParsPool.withPool
import static groovyx.gpars.dataflow.Dataflow.task

/**
 * User: westphal
 * Date: 07.08.13
 * Time: 08:40
 */

/**
 * A simple mashup sample, downloads content of three websites and checks how many of them refer to Groovy.
 */
final List urls = ['http://www.dzone.com', 'http://www.jroller.com', 'http://www.theserverside.com']


def downloadPage = { def url ->
    def page = new DataflowVariable()
    task {
        println "Started downloading from $url"
        page << url.toURL().text
        println "Done downloading from $url"
    }
    return page
}
task {
    def pages = urls.collect { downloadPage(it) }
    withPool {
        println "Number of Groovy sites today: " +
                (pages.findAllParallel {
                    it.val.toUpperCase().contains 'GROOVY'
                }).size()
    }
}.join()
