package org.segment.d

import org.segment.d.dialect.MySQLDialect
import spock.lang.Specification

class RecordTest extends Specification {

    static enum Sex {
        MALE, FEMALE
    }

    static class UserDTO extends Record<UserDTO> {
        Integer id
        String name

        @Override
        String pk() {
            'id'
        }
    }

    static class StudentBaseInfoDTO extends Record<StudentBaseInfoDTO> {
        Integer id
        String studentName
        Integer age
        Sex sex
        String[] colors
        Integer[] arr0
        Double[] arr1

        @Override
        String pk() {
            'id'
        }

//        @Override
//        D useD() {
//        }
    }

    def 'base method'() {
        given:
        def user = new UserDTO(id: 1, name: 'kerry')
        def student = new StudentBaseInfoDTO(id: 1, studentName: 'kerry', age: 32, sex: Sex.FEMALE,
                colors: ['red', 'blue'], arr0: [1, 2], arr1: [1.1d, 2.2d])
        student.setProperties([age: 33])
        student.prop('color', 'blue')
        student.prop('color2', 'red')
        student.removeProp('color2')
        UserDTO user2 = student as UserDTO
        UserDTO user3 = student.asType(UserDTO, [name: 'studentName'])
        expect:
        user.tbl() == 'user'
        student.tbl() == 'student_base_info'

        user.tableNameReset('x').tbl() == 'x'
        user.tableFields() == 'id,name'
        student.tableFields() == 'id,student_name,age,sex,colors,arr0,arr1'
        student.age == 33
        student.prop('color') == 'blue'
        student.prop('color2') == null

        student.rawProps() == [id    : 1, studentName: 'kerry', age: 33, sex: Sex.FEMALE,
                               colors: ['red', 'blue'], arr0: [1, 2], arr1: [1.1d, 2.2d]]
        student.rawProps(true) == [id    : 1, studentName: 'kerry', age: 33, sex: Sex.FEMALE,
                                   colors: ['red', 'blue'], arr0: [1, 2], arr1: [1.1d, 2.2d], color: 'blue']
        user2.id == 1
        user3.name == 'kerry'
    }

    def 'crud method'() {
        given:
        def ds = Ds.h2mem('test')
        def d = new D(ds, new MySQLDialect())
        d.exe('''
create table student_base_info(
id int,
student_name varchar(50),
age int,
sex varchar(6),
colors varchar(50),
arr0 varchar(50),
arr1 varchar(50)
)
''')
        def student = new StudentBaseInfoDTO(id: 1, studentName: 'kerry', age: 32, sex: Sex.MALE,
                colors: ['red', 'blue'], arr0: [1, 2], arr1: [1.1d, 2.2d])
        student.d = d
        10.times {
            student.id = it + 1
            student.add()
        }

        student.id = 11
        student.add()
        student.age = 36
        student.update()
        student.delete()
        new StudentBaseInfoDTO(id: 11).withD(d).deleteAll()

        def x = new StudentBaseInfoDTO(id: 1, d: d).
                queryFields('studentName').queryFieldsExclude('id').one()
        int givenId = 2
        def y = new StudentBaseInfoDTO(d: d).where(givenId != 0, 'id=?', givenId).one()
        def z = new StudentBaseInfoDTO(id: 3, d: d)
        z.load()
        def queryList = new StudentBaseInfoDTO(d: d).whereIn('id', [3, 4, 5], false).
                whereNotIn('id', [3], false).list()
        def queryList2 = new StudentBaseInfoDTO(d: d).whereIn('id', [3, 4, 5], false).
                whereReset().where('id>?', 6).orderBy('id desc').list(2)
        def pager = new StudentBaseInfoDTO(d: d, pageNum: 1, pageSize: 2).where('id>4').listPager()
        expect:
        x.studentName == 'kerry'
        y.id == 2
        z.id == 3
        queryList.size() == 2
        queryList[0].id == 4
        queryList2.size() == 2
        queryList2[0].id == 10
        pager.totalCount == 6
        pager.list[0].id == 5

        cleanup:
        ds.closeConnect()
    }

    def 'transaction support'() {
        given:
        def ds = Ds.h2mem('test')
        def d = new D(ds, new MySQLDialect())
        d.exe('''
create table student_base_info(
id int,
student_name varchar(50),
age int
)
''')
        def student = new StudentBaseInfoDTO(id: 1, studentName: 'kerry', age: 32)
        student.d = d

        ds.sql.withTransaction {
            10.times {
                student.id = it + 1
                student.add()
            }
        }

        def totalCount = new StudentBaseInfoDTO().withD(d).
                queryFields('id').noWhere().list().size()

        expect:
        totalCount == 10

        cleanup:
        ds.closeConnect()
    }
}
