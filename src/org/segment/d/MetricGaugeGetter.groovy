package org.segment.d


import groovy.transform.CompileStatic

@CompileStatic
interface MetricGaugeGetter {
    Object get()
}