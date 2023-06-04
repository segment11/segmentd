package org.segment.d

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

    def 'pager transfer'() {
        given:
        def pager = new Pager(1, 10)
        pager.totalCount = 3
        pager.list = [1, 2, 3]
        println pager
        def pager2 = pager.transfer { int i -> i.toString() }
        expect:
        pager.totalCount == pager2.totalCount
        pager.list.size() == pager2.list.size()
        pager2.list == ['1', '2', '3']
    }
}
