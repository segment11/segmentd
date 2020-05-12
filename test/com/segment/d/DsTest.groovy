package com.segment.d

import com.alibaba.druid.pool.DruidDataSource
import org.h2.tools.Server
import spock.lang.Specification

class DsTest extends Specification {
    def 'h2 memory mode connect'() {
        given:
        def ds = Ds.h2mem('test')
        expect:
        ds.sql.firstRow('select 1 as a').a == 1
        cleanup:
        ds.closeConnect()
    }

    def 'h2 local mode connect'() {
        given:
        def ds = Ds.h2Local('~/test')
        expect:
        ds.sql.firstRow('select 1 as a').a == 1
        cleanup:
        ds.closeConnect()
    }

    def 'h2 tcp mode connect with pool and collect stats'() {
        given:
        String[] arr = ["-tcp", "-tcpAllowOthers", "-tcpPort", "8043"]
        def server = Server.createTcpServer(arr).start()
        println 'h2 tcp server started'

        def register = new MetricGaugeRegister() {
            Map<String, MetricGaugeGetter> gauges = [:]

            @Override
            void register(String name, MetricGaugeGetter getter) {
                gauges[name] = getter
            }
        }
        def ds = Ds.dbType(Ds.DBType.h2).dataSourceInitHandler { DruidDataSource ds ->
            ds.testOnBorrow = true
        }.filters('mergeStat').
                collectDruidDataSourceStatsIntervalMillis(5 * 1000).
                maxStatSqlSize(10).
                addSqlStatCollectProperty('executeSuccessCount').
                metricGaugeRegister(register).
                urlParam('useUnicode', true).
                connectWithPool('127.0.0.1', 8043, '~/test', 'sa', null, 1, 2).
                registerDataSourceStats()
        expect:
        ds.sql.firstRow('SELECT 1 AS a').a == 1

        when:
        Thread.sleep(1000 * 10)
        then:
        def stat = ds.findSqlStatByKeyword('AS')
        println stat?.getData()
        def statList = ds.findSqlStatListByKeyword('AS')
        statList?.each {
            println it.getData()
        }
        register.gauges.each { k, v ->
            println k + ': ' + v.get()
        }
        cleanup:
        ds.closeConnect()
        server.stop()
        println 'h2 tcp server stopped'
    }
}
