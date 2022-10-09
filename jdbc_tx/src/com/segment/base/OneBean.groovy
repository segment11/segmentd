package com.segment.base

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.segment.d.D
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.DefaultTransactionDefinition

@CompileStatic
@Slf4j
class OneBean {
    D d

    PlatformTransactionManager tm

    Integer doQuery() {
        def r = d.one('select 1 as a')
        r.get('a')
    }

    Integer doBatch() {
        def td = new DefaultTransactionDefinition()
        td.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED)
        def status = tm.getTransaction(td)
        try {
            5.times {
                new User(name: 'xx', age: 0).add()
            }
            // too long for name when insert
            new User(name: 'yy'.padRight(30, ' '), age: 0).add()
            tm.commit(status)
        } catch (Exception e) {
            log.error('error batch', e)
            tm.rollback(status)
        }

        def r = d.one('select count(id) as a from user')
        r.get('a')
    }
}
