package com.segment.d

import spock.lang.Specification

class PagerTest extends Specification {

    def 'pagination count'() {
        given:
        def pager = new Pager(2, 10)
        pager.totalCount = 101
        println pager
        expect:
        pager.totalPage == 11
        pager.start == 10
        pager.end == 20
        pager.hasNext()
        pager.hasPre()
    }
}
