import prologue
import ../src/amysql, asyncdispatch
import strformat
import ../src/amysql/db_pool

const database_name = "test"
const port: int = 3306
const host_name = "127.0.0.1"
const user_name = "test_user"
const pass_word = "123456"

var pool {.threadvar.}: DBPool

pool = waitFor newDBPool(fmt"mysqlx://{user_name}:{pass_word}@{host_name}/{database_name}?minPoolSize=2&maxPoolSize=4")

proc hello*(ctx: Context) {.async.} =
  let a = await pool.rawQuery("drop table if exists test_dt")
  resp "<h1>Hello, Prologue!</h1>"

let settings = newSettings()
var app = newApp(settings = settings)
app.addRoute("/", hello)
app.run()