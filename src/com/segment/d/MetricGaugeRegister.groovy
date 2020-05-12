package com.segment.d

import groovy.transform.CompileStatic

@CompileStatic
interface MetricGaugeRegister {
    void register(String name, MetricGaugeGetter getter)
}