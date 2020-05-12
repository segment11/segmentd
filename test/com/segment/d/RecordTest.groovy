package com.segment.d

import spock.lang.Specification

class RecordTest extends Specification {

    private static class UserDTO extends Record {
        Integer id
        String name

        @Override
        String pk() {
            'id'
        }
    }

    private static class StudentBaseInfoDTO extends Record {
        Integer id
        String studentName
        Integer age

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
        def student = new StudentBaseInfoDTO(id: 1, studentName: 'kerry', age: 32)
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
        student.tableFields() == 'id,student_name,age'
        student.age == 33
        student.prop('color') == 'blue'
        student.prop('color2') == null

        student.rawProps() == [id: 1, studentName: 'kerry', age: 33]
        student.rawProps(true) == [id: 1, studentName: 'kerry', age: 33, color: 'blue']
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
age int
)
''')
        def student = new StudentBaseInfoDTO(id: 1, studentName: 'kerry', age: 32)
        student.d = d
        10.times {
            student.id = it + 1
            student.add()
        }
        StudentBaseInfoDTO x = new StudentBaseInfoDTO(id: 1, d: d).
                queryFields('studentName').queryFieldsExclude('id').one()
        int givenId = 2
        StudentBaseInfoDTO y = new StudentBaseInfoDTO(d: d).where(givenId != 0, 'id=?', givenId).one()
        StudentBaseInfoDTO z = new StudentBaseInfoDTO(id: 3, d: d)
        z.load()
        def queryList = new StudentBaseInfoDTO(d: d).whereIn('id', [3, 4, 5], false).
                whereNotIn('id', [3], false).loadList() as List<StudentBaseInfoDTO>
        def queryList2 = new StudentBaseInfoDTO(d: d).whereIn('id', [3, 4, 5], false).
                whereReset().where('id>?', 6).orderBy('id desc').loadList(2) as List<StudentBaseInfoDTO>
        def pager = new StudentBaseInfoDTO(d: d, pageNum: 1, pageSize: 2).where('id>4').loadPager() as Pager<StudentBaseInfoDTO>
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
}
