package org.segment.d

import spock.lang.Specification

class DTest extends Specification {

    static class User {
        Integer id
        String name

        @Override
        String toString() {
            '' + id + ':' + name
        }
    }

    static class Student {
        Integer id
        String studentName

        @Override
        String toString() {
            '' + id + ':' + studentName
        }
    }

    def 'underline to camel'() {
        expect:
        D.toCamel('student_name') == 'studentName'
        D.toCamel('student_name', false) == 'StudentName'
    }

    def 'camel to underline'() {
        expect:
        D.toUnderline('studentName') == 'student_name'
        D.toUnderline(['studentName': 1]) == ['student_name': 1]
    }

    def 'bean to map'() {
        expect:
        D.bean2map(new User(id: 1, name: 'kerry')) == [id: 1, name: 'kerry']
        D.bean2map(new User(id: 1, name: 'kerry'), ['name'] as Set) == [id: 1]
    }

    def 'map to bean'() {
        given:
        User user = D.map2bean([id: 1, name: 'kerry'], User)
        expect:
        user.name == 'kerry'
    }

    def 'ddl execute'() {
        given:
        def ds = Ds.h2mem('test')
        def d = new D(ds, new MySQLDialect())
        d.exe('''
create table a(
id int,
name varchar(50)
)
''')
        expect:
        d.one('select 1 as a') == [a: 1]
        cleanup:
        ds.closeConnect()
    }

    def 'insert and update and query'() {
        given:
        def ds = Ds.h2mem('test')
        def d = new D(ds, new MySQLDialect())
        d.exe('''
create table a(
id int,
student_name varchar(50)
)
''')
        10.times {
            d.add([id: it + 1, studentName: 'kerry' + it], 'a')
            d.add([id: it + 10, student_name: 'kerry' + it], 'a', false)
            d.add(new Student(id: it + 20, studentName: 'kerry' + it), 'a')
        }
        d.update([id: 2, studentName: 'kerry2!'], 'a', 'id')
        expect:
        d.one('select count(id) as a from a') == [a: 30]
        d.query('select * from a limit 1')[0] == [id: 1, studentName: 'kerry0']
        d.query('select * from a limit 1', Student)[0].studentName == 'kerry0'
        d.query('select * from a where id = ?', [2], Student)[0].studentName == 'kerry2!'
        cleanup:
        ds.closeConnect()
    }

    def 'pagination query'() {
        given:
        def ds = Ds.h2mem('test')
        def d = new D(ds, new MySQLDialect())
        d.exe('''
create table a(
id int,
student_name varchar(50)
)
''')
        55.times {
            d.add([id: it + 1, studentName: 'kerry' + (it + 1)], 'a')
        }
        def pager = d.pagination('select * from a', null, HashMap, 2, 10)
        println pager
        expect:
        pager.totalCount == 55
        pager.totalPage == 6
        pager.hasPre()
        pager.hasNext()
        cleanup:
        ds.closeConnect()
    }
}
