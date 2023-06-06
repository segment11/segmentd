package org.segment.d

import groovy.sql.Sql
import org.h2.jdbcx.JdbcDataSource
import org.segment.d.dialect.MySQLDialect
import org.segment.d.json.JSONFiled
import spock.lang.Specification

class DTest extends Specification {

    static class Grade implements JSONFiled {
        Integer level
    }

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
        Grade grade

        @Override
        String toString() {
            '' + id + ':' + studentName + ':' + grade?.level
        }
    }

    def 'base'() {
        given:
        def d1 = new D(Sql.newInstance('jdbc:h2:mem:test1'), new MySQLDialect())
        def ds = new JdbcDataSource()
        ds.url = 'jdbc:h2:mem:test1'
        def d2 = new D(ds, new MySQLDialect())
        d1.addReturnPk(true).addSkipProperties('metaClass')
        expect:
        d1.db != null
        d1.dialect.isLimitSupport()
        d1.one('select 1 as a') == [a: 1]
        d2.one('select 1 as a') == [a: 1]
        cleanup:
        d1.close()
    }

    def 'underline to camel'() {
        expect:
        D.toCamel(null) == null
        D.toCamel('student_name') == 'studentName'
        D.toCamel('student_name', false) == 'StudentName'
    }

    def 'camel to underline'() {
        expect:
        D.toUnderline((String) null) == null
        D.toUnderline('studentName') == 'student_name'
        D.toUnderline(['studentName': 1]) == ['student_name': 1]
    }

    def 'bean to map'() {
        given:
        def ex = new Expando()
        ex.setProperty('id', 1)
        expect:
        D.bean2map(ex) == [id: 1]
        D.bean2map(new User(id: 1, name: 'kerry')) == [id: 1, name: 'kerry']
        D.bean2map(new User(id: 1, name: 'kerry'), ['name'] as Set) == [id: 1]
    }

    def 'map to bean'() {
        given:
        User user = D.map2bean([id: 1, name: 'kerry'], User)
        Expando ex = D.map2bean([id: 1], Expando)
        expect:
        user.name == 'kerry'
        ex.getProperty('id') == 1
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
student_name varchar(50),
grade varchar(200)
)
''')
        10.times {
            d.add([id: it + 1, studentName: 'kerry' + it], 'a')
            d.add([id: it + 10, student_name: 'kerry' + it], 'a', false)
            d.add(new Student(id: it + 20, studentName: 'kerry' + it, grade: new Grade(level: 1)), 'a')
        }
        d.update([id: 2, studentName: 'kerry2!'], 'a', 'id')
        expect:
        d.one('select count(id) as a from a') == [a: 30]
        d.query('select * from a limit 1')[0] == [id: 1, studentName: 'kerry0']
        d.query('select * from a limit 1', Student)[0].studentName == 'kerry0'
        d.one('select * from a where id = ?', [2], Student).studentName == 'kerry2!'
        d.one('select * from a where id = ?', [21], Student).grade.level == 1
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
