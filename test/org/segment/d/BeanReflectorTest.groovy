package org.segment.d

import spock.lang.Specification

class BeanReflectorTest extends Specification {

    private static interface T1 {
        String hi()
    }

    private static class T1Impl implements T1 {
        String name

        @Override
        String hi() {
            'hi ' + name
        }
    }

    def 'method access'() {
        given:
        def reflector = BeanReflector.get(T1Impl, 'hi')
        println reflector.returnType
        expect:
        BeanReflector.getMaCached(T1Impl) == BeanReflector.getMaCached(T1Impl)
        reflector.invoke(new T1Impl(name: 'kerry')) == 'hi kerry'
    }

    def 'field get'() {
        expect:
        BeanReflector.getClassFields(T1Impl).collect { it.name }.contains('name')
    }
}
