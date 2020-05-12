package com.segment.d

import com.alibaba.druid.pool.DruidDataSource
import groovy.transform.CompileStatic

@CompileStatic
interface DruidDataSourceInitHandler {
    void set(DruidDataSource dataSource)
}