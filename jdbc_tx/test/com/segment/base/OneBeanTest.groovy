package com.segment.base

import groovy.util.logging.Slf4j
import org.h2.tools.Server
import org.junit.AfterClass
import org.junit.BeforeClass
import org.segment.d.D
import spock.lang.Specification

@Slf4j
class OneBeanTest extends Specification {
    private Server server

    @AfterClass
    void after() {
        ContextHolder.instance.close()
        if (server) {
            server.stop()
            log.info 'h2 db tcp server stopped'
        }
    }

    @BeforeClass
    void before() {
        server = Server.createTcpServer('-tcp', '-tcpAllowOthers', '-tcpPort', '8082', '-ifNotExists').start()
        log.info 'h2 db tcp server started'
        ContextHolder.instance.init(this.class.classLoader, 'classpath*:conf.groovy')
    }

    def 'testQuery'() {
        given:
        def ctx = ContextHolder.instance
        def oneBean = ctx.getBean('oneBean') as OneBean
        expect:
        oneBean.doQuery() == 1
    }

    def 'testBatch'() {
        given:
        def ctx = ContextHolder.instance
        def d = ctx.getBean('d') as D
        d.exe('''
create table if not exists user (
id int auto_increment primary key, 
name varchar(20),
age int
)
''')
        d.exe('delete from user')
        10.times {
            d.exe('insert into user (name, age) values (?,?)', ['kerry' + it, 18 + it])
        }
        def oneBean = ctx.getBean('oneBean') as OneBean
        expect:
        oneBean.doBatch() == 10
    }
}
