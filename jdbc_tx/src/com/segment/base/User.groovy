package com.segment.base

import groovy.transform.CompileStatic
import org.segment.d.D
import org.segment.d.Record

@CompileStatic
class User extends Record<User> {
    Integer id
    String name
    Integer age

    @Override
    String pk() {
        'id'
    }

    @Override
    D useD() {
        ContextHolder.instance.getBean('d') as D
    }
}
