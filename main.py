import psycopg2 as pg


def commit(*args, need_print=0):
    try:
        conn = pg.connect(database='netology', user='netology', password='netology',
                host='localhost', port=5432)
        cur = conn.cursor()
        cur.execute(*args)
        if need_print==0:
            cur.close()
            conn.commit()
            print('Завершено успешно.')
        else:
            row = cur.fetchone()
            while row is not None:
                print(row[0])
                row = cur.fetchone()
            cur.close()
            conn.commit()
    except (Exception, pg.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_db(): #создает таблицы
    commands = (
        '''CREATE TABLE IF NOT EXISTS courses (
            course_id SERIAL PRIMARY KEY,
            course_name VARCHAR(100) NOT NULL
            );''',
        '''CREATE TABLE IF NOT EXISTS students (
            student_id SERIAL PRIMARY KEY,
            student_name VARCHAR(100) NOT NULL,
            gpa DECIMAL(10, 2),
            birth DATE
            );''',
        '''CREATE TABLE IF NOT EXISTS student_course (
            id serial PRIMARY KEY,
            student_id INTEGER REFERENCES students(student_id),
            course_id INTEGER REFERENCES  courses(course_id));''')
    for command in commands:
        commit(command)


def add_course(course): #просто создает курс
    sql = """INSERT INTO courses(course_name)
             VALUES(%s) RETURNING course_id;"""
    commit(sql, (course,))


def add_student(student): #просто создает студента
    sql = """INSERT INTO students(student_name, gpa, birth)
             VALUES(%(student_name)s, %(gpa)s, %(birth)s );"""
    try:
        conn = pg.connect(database='netology', user='netology', password='netology',
                host='localhost', port=5432)
        cur = conn.cursor()
        cur.executemany(sql, (student,))
        conn.commit()
        cur.close()
        print(f'Студент успешно добавлен.')
    except (Exception, pg.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_student(student_id): #возвращает студента по его id
    sql = '''SELECT student_name FROM students WHERE student_id = %s;'''
    commit(sql, student_id, need_print=1)


def add_students(course_id, students): # записывает студентов на курс 
    sql = """
    INSERT INTO student_course(course_id, student_id) values (%s, %s)"""
    for student_id in students:
        commit(sql, (course_id, student_id))


def get_students(course_id): # возвращает студентов определенного курса
    sql = """select student_name from student_course 
    JOIN students on students.student_id = student_course.student_id
    where course_id = %s ORDER BY student_name"""
    print("На курс записаны следующие студенты: ")
    commit(sql, course_id, need_print=1,)


if __name__ == '__main__':
    # create_db()
    # add_course('Python')
    # add_course('Sql')
    # add_student({'student_name':'Kate', 'gpa':'6.6', 'birth':'1999-03-04'})
    # add_student({'student_name':'Bob', 'gpa':'9.6', 'birth':'1989-04-04'})
    # add_student({'student_name':'Dan', 'gpa':'4.6', 'birth':'1989-03-22'})
    # add_students('2',('1', '3'))
    # add_students('1',('2'))
    get_student('2')
    get_students('2')
    get_students('1')
