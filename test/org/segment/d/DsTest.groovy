package org.segment.d

import com.alibaba.druid.pool.DruidDataSource
import com.github.kevinsawicki.http.HttpRequest
import io.prometheus.client.exporter.HTTPServer
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
        def ds2 = Ds.h2LocalWithPool('~/test', 'ds_h2_local')
        expect:
        ds.sql.firstRow('select 1 as a').a == 1
        ds2.sql.firstRow('select 1 as a').a == 1
        cleanup:
        ds.closeConnect()
        ds2.closeConnect()
    }

    def 'user define db type connect'() {
        given:
        Ds.register('h2_spec', 'org.h2.Driver') { String ip, int port, String db ->
            "jdbc:h2:mem:${db}".toString()
        }
        def ds = Ds.dbType('h2_spec').connect(null, 0, 'test', null, null)
        expect:
        ds.sql.firstRow('select 1 as a').a == 1
        cleanup:
        ds.closeConnect()
    }

    def 'h2 tcp mode connect with pool and collect stats'() {
        String[] arr = ["-tcp", "-tcpAllowOthers", "-ifNotExists", "-tcpPort", "8043"]
        def server = Server.createTcpServer(arr).start()
        println 'h2 tcp server started'
        def ds = Ds.dbType(Ds.DBType.h2).cacheAs('test_ds').dataSourceInitHandler { DruidDataSource dsInner ->
            dsInner.testOnBorrow = true
            dsInner.validationQuery = 'select 1'
        }.filters('mergeStat').
                maxStatSqlSize(10).
                addSqlStatCollectProperty('executeSuccessCount').
                urlParam('useUnicode', true).
                connectWithPool('127.0.0.1', 8043, '~/test', 'sa', null, 1, 2).
                extendLabelValue('ip', '127.0.0.1').export()

        def metricsServer = new HTTPServer(7700)
        ds.sql.executeUpdate('create table if not exists t(name varchar(20))')
        ds.sql.executeUpdate('insert into t(name) values(?)', ['kerry'])
        expect:
        Ds.one('test_ds') != null
        !ds.dataSource.isClosed()
        ds.sql.firstRow('SELECT 1 AS a').a == 1
        ds.sql.firstRow('SELECT name from t where name = ?', ['kerry']).name == 'kerry'

        println HttpRequest.get('http://127.0.0.1:7700/metrics').body()

        println 'sql set:'
        ds.sqlHashHolder().each { k, v ->
            println '' + k + ': ' + v
        }
        def stat = ds.findSqlStatByKeyword('as')
        println 'sql query by keyword: as'
        println stat?.getData()
        println 'sql query by keyword select Name'
        def statList = ds.findSqlStatListByKeyword('select NAME')
        if (statList) {
            statList.each {
                println it.getData()
            }
        }
        cleanup:
        metricsServer.close()
        ds.closeConnect()
        server.stop()
        println 'h2 tcp server stopped'
    }

    void cleanup() {
        Ds.remove()
        Ds.disconnectOne()
        Ds.disconnectAll()
    }
}
