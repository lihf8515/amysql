import amysql, asyncdispatch
import unittest
import net
import strformat
import amysql/db_pool
import times

const database_name = "test"
const port: int = 3306
const host_name = "127.0.0.1"
const user_name = "test_user"
const pass_word = "123456"


suite "pool connnection":
  test "connnection":
    let pool = waitFor newDBPool(fmt"mysqlx://{user_name}:{pass_word}@{host_name}/{database_name}?minPoolSize=2&maxPoolSize=4")
    discard waitFor pool.rawQuery("drop table if exists test_dt")
    discard waitFor pool.rawQuery("CREATE TABLE test_dt(col DATE NOT NULL)")
    let r = waitFor pool.rawQuery("SHOW TABLES LIKE 'test_dt'")
    check r.rows.len == 1
    # Insert values using the binary protocol
    let insrow = waitFor pool.prepare("INSERT INTO test_dt (col) VALUES (?),(?)")
    let d1 = initDate(1,1.Month,2020)
    let d2 = initDate(31,12.Month,2020)
    discard waitFor pool.query(insrow, d1, d2)
    waitFor pool.finalize(insrow)

    # Read them back using the text protocol
    let r1 = waitFor pool.rawQuery("SELECT * FROM test_dt")
    check r1.rows[0][0] == "2020-01-01"
    check r1.rows[1][0] == "2020-12-31"

    # Now read them back using the binary protocol
    let rdtab = waitFor pool.prepare("SELECT * FROM test_dt")
    let r2 = waitFor pool.query(rdtab)

    check r2.rows[0][0] == d1
    check r2.rows[1][0] == d2

    waitFor pool.finalize(rdtab)

    discard waitFor pool.rawQuery("drop table test_dt")
    waitFor pool.close()
  