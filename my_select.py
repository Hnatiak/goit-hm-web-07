from sqlalchemy import func, select, desc, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session

def select_01():
    """
    SELECT
        s.id,
        s.fullname,
        AVG(g.grade) as average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """

    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
            .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result

def select_02():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) as average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    where g.subject_id = 1
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """

    # result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
    #         .select_from(Grade).join(Student).filter(Grade.subjects_id == 1).group_by(Student.id).order_by(
    #     desc('average_grade')).limit(1).all()
    # return result
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subjects_id == 1).group_by(Student.id).order_by(
        desc('average_grade')).limit(1).all()
    return result

def select_03():
    """
    SELECT
        g.subject_id,
        grp.name AS group_name,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    JOIN groups grp ON s.group_id = grp.id
    WHERE g.subject_id = 1
    GROUP BY g.subject_id, grp.name
    ORDER BY g.subject_id, average_grade DESC;
    """

    result = session.query(Grade.subjects_id, Group.name.label('group_name'),
                           func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).join(Group).filter(Grade.subjects_id == 1) \
        .group_by(Grade.subjects_id, Group.name).order_by(Grade.subjects_id, desc('average_grade')).all()
    return result

def select_04():
    """
    SELECT ROUND(AVG(grade), 2) AS average_grade_overall
    FROM grades;
    """

    result = session.query(func.round(func.avg(Grade.grade), 2).label('average_grade_overall')).first()
    return result

def select_05():
    """
    SELECT
        s.id AS subject_id,
        s.name AS subject_name,
        t.fullname AS teacher_name
    FROM subjects s
    JOIN teachers t ON s.teacher_id = t.id
    WHERE t.fullname = 'Kelly Prince';
    """

    result = session.query(Subject.id.label('subject_id'), Subject.name.label('subject_name'),
                           Teacher.fullname.label('teacher_name')) \
             .join(Teacher, Subject.teacher_id == Teacher.id) \
             .filter(Teacher.fullname == 'Kelly Prince').all()
    return result

def select_06():
    """
    SELECT
        s.id AS student_id,
        s.fullname AS student_name,
        g.name AS group_name
    FROM students s
    JOIN groups g ON s.group_id = g.id
    WHERE g.name = 'bank';
    """

    result = session.query(Student.id.label('student_id'), Student.fullname.label('student_name'),
                           Group.name.label('group_name')) \
        .join(Group, Student.group_id == Group.id) \
        .filter(Group.name == 'bank').all()
    return result

def select_07():
    """
    SELECT
        s.id AS student_id,
        s.fullname AS student_name,
        g.grade,
        sub.name AS subject_name
    FROM grades g
    JOIN students s ON g.student_id = s.id
    JOIN subjects sub ON g.subject_id = sub.id
    JOIN groups grp ON s.group_id = grp.id
    WHERE
        grp.name = 'bank'
        AND sub.id = 2;
    """

    result = session.query(Student.id.label('student_id'), Student.fullname.label('student_name'),
                           Grade.grade, Subject.name.label('subject_name')) \
        .join(Grade, Student.id == Grade.student_id) \
        .join(Subject, Grade.subjects_id == Subject.id) \
        .join(Group, Student.group_id == Group.id) \
        .filter(and_(Group.name == 'bank', Subject.id == 2)).all()
    return result

def select_08():
    """
    SELECT
        t.fullname AS teacher_name,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM teachers t
    JOIN subjects s ON t.id = s.teacher_id
    JOIN grades g ON s.id = g.subject_id
    GROUP BY t.fullname;
    """

    result = session.query(Teacher.fullname.label('teacher_name'),
                           func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .join(Subject, Teacher.id == Subject.teacher_id) \
        .join(Grade, Subject.id == Grade.subjects_id) \
        .group_by(Teacher.fullname).all()
    return result

def select_09():
    """
    SELECT
        s.id AS student_id,
        s.fullname AS student_name,
        sub.name AS subject_name
    FROM students s
    JOIN grades g ON s.id = g.student_id
    JOIN subjects sub ON g.subject_id = sub.id
    WHERE s.fullname = 'Kathy Ayala';
    """

    result = session.query(Student.id.label('student_id'), Student.fullname.label('student_name'), Subject.name.label('subject_name')) \
             .join(Grade, Student.id == Grade.student_id) \
             .join(Subject, Grade.subjects_id == Subject.id) \
             .filter(Student.fullname == 'Kathy Ayala').all()
    return result


def select_10():
    """
    SELECT
        s.fullname AS student_name,
        t.fullname AS teacher_name,
        sub.name AS subject_name
    FROM students s
    JOIN grades g ON s.id = g.student_id
    JOIN subjects sub ON g.subject_id = sub.id
    JOIN teachers t ON sub.teacher_id = t.id
    WHERE
        s.id = 1
        AND t.id = 2;
    """

    result = session.query(Student.fullname.label('student_name'), Teacher.fullname.label('teacher_name'), Subject.name.label('subject_name')) \
             .join(Grade, Student.id == Grade.student_id) \
             .join(Subject, Grade.subjects_id == Subject.id) \
             .join(Teacher, Subject.teacher_id == Teacher.id) \
             .filter(Student.id == 1) \
             .filter(Teacher.id == 2) \
             .group_by(Student.fullname, Teacher.fullname, Subject.name).all()
    return result


if __name__ == '__main__':
    print(select_01())
    print(select_02())
    print(select_03())
    print(select_04())
    print(select_05())
    print(select_06())
    print(select_07())
    print(select_08())
    print(select_09())
    print(select_10())