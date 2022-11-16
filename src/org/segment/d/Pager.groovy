package org.segment.d

import groovy.transform.CompileStatic

@CompileStatic
class Pager<T> {
    int pageNum = 1
    int pageSize = 10
    int totalCount = 0
    List<T> list

    Pager(int pageNum, int pageSize) {
        this.pageNum = pageNum
        this.pageSize = pageSize
    }

    public <V> Pager<V> transfer(Closure<V> closure) {
        def pager = new Pager(this.pageNum, this.pageSize)
        pager.totalCount = this.totalCount
        if (this.list) {
            pager.list = new LinkedList<V>()
            for (T one in this.list) {
                pager.list.add(closure.call(one))
            }
        }
        pager
    }

    int getTotalPage() {
        int r = (totalCount / pageSize) as int
        totalCount % pageSize == 0 ? r : r + 1
    }

    boolean hasNext() {
        pageNum < totalPage
    }

    boolean hasPre() {
        pageNum > 1 && totalPage > 1
    }

    int getStart() {
        (pageNum - 1) * pageSize
    }

    int getEnd() {
        if (totalCount <= pageSize || (!hasNext())) {
            return totalCount
        }
        pageNum * pageSize
    }

    @Override
    String toString() {
        def sb = new StringBuilder()
        sb << pageNum
        sb << '/'
        sb << totalPage
        sb << ' Total:'
        sb << totalCount
        sb << ' PageSize:'
        sb << pageSize
        if (list != null) {
            sb << ' Page List:'
            for (one in list) {
                sb << one.toString()
                sb << ','
            }
        }
        sb.toString()
    }
}
