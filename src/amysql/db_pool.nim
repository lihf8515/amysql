
{.experimental: "dotOperators".}

import ./conn
import locks
import times
import asyncdispatch
import ../amysql
import uri
import strutils
import macros
import strformat
import asyncnet
import sets,hashes

type 
  DBPool* = ref DBPoolObj
  DBPoolObj = object
    freeConn: HashSet[DBConn]
    closed: bool
    maxPoolSize: int
    numOpen: int
    maxLifetime: Duration # maximum amount of time a connection may be reused
    maxIdleTime: Duration # maximum amount of time a connection may be idle before being closed
    openUriStr: Uri
    reader:ptr Channel[DBResult]
   
  DBConn* = ref DBConnObj
  DBSqlPrepared* = object
    pstmt:SqlPrepared
    dbConn:DBConn
  StmtKind = enum
    text,
    binary
  InterfaceKind {.pure.} = enum
    none,query,prepare,finalize,reset
  DBStmt {.pure.} = object
    case kind:StmtKind
    of text:
    textVal:string
    of binary:
    binVal:SqlPrepared
    
    case met:InterfaceKind
    of InterfaceKind.query:
      params:seq[SqlParam]
    of InterfaceKind.none:
      discard
    of InterfaceKind.prepare:
      q:string
    of InterfaceKind.finalize,InterfaceKind.reset:
      pstmt:SqlPrepared
  DBResultKind = enum
    resultsetText,resultsetBinary,pstmt,none
  DBResult {.pure.} = object
    case kind:DBResultKind
    of resultsetText:
      textVal:ResultSet[string]
    of resultsetBinary:
      binVal:ResultSet[ResultValue]
    of pstmt:
      pstmt:SqlPrepared
    of none:
      discard
    
  DBConnObj = object
    pool: DBPool
    createdAt: DateTime #time.Time
    lock: Lock  # guards following
    conn: Connection
    needReset: bool # The connection session should be reset before use if true.
    closed: bool
    # guarded by db.mu
    inUse: bool
    returnedAt: DateTime # Time the connection was created or returned.
    reader: ptr Channel[DBStmt]
    writer: ptr Channel[DBResult] # same as pool's reader

proc newDBConn*(conn: Connection,writer:ptr Channel[DBResult]): DBConn =
  new result
  result.createdAt = now()
  result.returnedAt = now()
  result.conn = conn
  result.reader = cast[ptr Channel[DBStmt]](
    allocShared0(sizeof(Channel[DBStmt]))
  )
  result.writer = writer
  result.reader[].open()

proc close*(self: DBConn) {.async.} =
  self.reader[].close
  deallocShared(self.reader)
  await self.conn.close

proc expired*(self: DBConn, timeout:Duration): bool = 
  if timeout <= DurationZero:
    return false
  return self.createdAt + timeout < now()

type Conext = object
  conn: DBConn

proc worker(ctx:Conext) =
  asyncdispatch.register(AsyncFD(ctx.conn.conn.socket.getFd()))
  while true:
    let tried = ctx.conn.reader[].tryRecv()
    if tried.dataAvailable:
      let st = tried.msg
      debugEcho fmt"dataAvailable:{$st}"
      case st.kind
      of StmtKind.text: 
        let r = waitFor ctx.conn.conn.rawQuery(st.textVal )
        debugEcho fmt"result:{$r}"
        let p = DBResult(kind:DBResultKind.resultsetText,textVal:r)
        ctx.conn.writer[].send(p)
      else: 
        case st.met:
        of InterfaceKind.none:
          discard
        of InterfaceKind.query:
          let r = waitFor ctx.conn.conn.query(st.binVal,st.params)
          debugEcho fmt"result:{$r}"
          let p = DBResult(kind:DBResultKind.resultsetBinary,binVal:r)
          ctx.conn.writer[].send(p)
        of InterfaceKind.prepare:
          let r = waitFor ctx.conn.conn.prepare(st.q)
          let p = DBResult(kind:DBResultKind.pstmt,pstmt:r)
          ctx.conn.writer[].send(p)
        of InterfaceKind.finalize:
          waitFor ctx.conn.conn.finalize(st.pstmt)
          let p = DBResult(kind:DBResultKind.none)
          ctx.conn.writer[].send(p)
        of InterfaceKind.reset:
          waitFor ctx.conn.conn.reset(st.pstmt)
          let p = DBResult(kind:DBResultKind.none)
          ctx.conn.writer[].send(p)

proc handleParams(query: string,minPoolSize,maxPoolSize:var int):string =
  var key, val: string
  var pos = 0
  for item in split(query,"&"):
    (key, val) = item.split("=")
    case key
    of "minPoolSize":
      minPoolSize = parseInt(val)
    of "maxPoolSize":
      maxPoolSize = parseInt(val)
    else:
      result.add key & '=' & val
      inc pos

proc hash(x: DBConn): Hash = 
  var h: Hash = 0
  h = h !& hash($x.createdAt)
  result = !$h

proc newDBPool*(uriStr: string | Uri): Future[DBPool] {.async.} = 
  ## min pool size
  ## max pool size
  ## max idle timeout exceed then close,affected freeConns,numOpen
  ## max open timeout
  ## max lifetime exceed then close,affected freeConns,numOpen
  new result
  result.freeConn.init()
  var uri = when uriStr is string: parseUri(uriStr) else: uriStr
  var minPoolSize,maxPoolSize:int
  uri.query = handleParams(uri.query,minPoolSize,maxPoolSize)
  debugEcho fmt"minPoolSize:{$minPoolSize},maxPoolSize:{$maxPoolSize}"
  result.maxPoolSize = maxPoolSize
  result.openUriStr = uri
  result.reader = cast[ptr Channel[DBResult]](
    allocShared0(sizeof(Channel[DBResult]))
  )
  result.reader[].open()
  for i in 0 ..< minPoolSize:
    var conn = await open(result.openUriStr)
    asyncdispatch.unregister(AsyncFD(conn.socket.getFd()))
    var dbConn = newDBConn(conn, result.reader)
    dbConn.pool = result
    result.freeConn.incl dbConn
    var ctx = Conext(conn: dbConn)
    var thread: Thread[Conext]
    createThread(thread, worker, ctx)
    inc result.numOpen
    
proc close*(self:DBPool): Future[void] {.async.}=
  self.reader[].close()
  deallocShared(self.reader)
  for conn in  self.freeConn:
    if conn.reader[].peek() != -1:
      conn.reader[].close()
  self.closed = true

proc fetchConn*(self: DBPool): Future[DBConn] {.async.} = 
  ## conn returns a newly-opened or new DBconn.
  if self.closed:
    discard
    # return nil, errDBClosed
  debugEcho fmt"self.freeConn:{$self.freeConn.len},self.numOpen:{$self.numOpen},self.maxPoolSize:{$self.maxPoolSize}"
  ## Out of free connections or we were asked not to use one. If we're not
  ## allowed to open any more connections, make a request and wait.
  while len(self.freeConn) == 0 and self.numOpen >= self.maxPoolSize:
    await sleepAsync(1)
  
  let lifetime = self.maxLifetime

  ## Prefer a free connection, if possible.
  let numFree = len(self.freeConn)
  if numFree > 0:
    result = self.freeConn.pop
    result.inUse = true
    if result.expired(lifetime):
      debugEcho fmt"expired: {$lifetime}"
      await result.close
      # return nil, driver.ErrBadConn

    ## Reset the session if required.
    # let err = conn.resetSession(ctx)
    # if err == driver.ErrBadConn:
    #   await conn.close()
    #   return nil, driver.ErrBadConn
    return result

  # no free conn, open new
  debugEcho "no free conn, open new"
  var conn = await open(self.openUriStr)
  asyncdispatch.unregister(AsyncFD(conn.socket.getFd()))
  result = newDBConn(conn,self.reader)
  result.pool = self
  inc self.numOpen
  debugEcho fmt"numOpen:{self.numOpen}"
  self.freeConn.incl result
  var ctx = Conext(conn: result)
  var thread: Thread[Conext]
  createThread(thread, worker, ctx)

proc rawQuery*(self: DBPool, query: string, onlyFirst:static[bool] = false): Future[ResultSet[string]] {.
               async.} =
  let conn = await self.fetchConn()
  debugEcho "fetchConn"
  conn.reader[].send(DBStmt(met:InterfaceKind.query,kind:StmtKind.text,textVal:query))
  debugEcho "reader send"
  let msg = conn.writer[].recv()
  debugEcho fmt"recv:{$msg}"
  self.freeConn.add conn
  return msg.textVal

proc query(self: DBPool, pstmt: DBSqlPrepared, params: seq[SqlParam]): Future[ResultSet[ResultValue]] {.async.} =
  let conn = pstmt.dbConn
  conn.reader[].send(DBStmt(met:InterfaceKind.query,kind:StmtKind.binary,binVal:pstmt.pstmt,params:params))
  debugEcho "reader send"
  let msg = conn.writer[].recv()
  debugEcho fmt"recv:{$msg}"
  self.freeConn.incl conn
  return msg.binVal

proc query*(self: DBPool, pstmt: DBSqlPrepared, params: varargs[SqlParam, asParam]): Future[ResultSet[ResultValue]] =
  let p:seq[SqlParam] = @params
  self.query(pstmt,p)

proc prepare*(self: DBPool, query: string): Future[DBSqlPrepared] {.async.} =
  let conn = await self.fetchConn()
  conn.reader[].send(DBStmt(met:InterfaceKind.prepare,kind:StmtKind.binary,q:query))
  let msg = conn.writer[].recv()
  return DBSqlPrepared(pstmt:msg.pstmt,dbConn:conn)

proc finalize*(self: DBPool, pstmt: DBSqlPrepared): Future[void] {.async.} =
  pstmt.dbConn.reader[].send(DBStmt(met:InterfaceKind.finalize,kind:StmtKind.binary,pstmt:pstmt.pstmt))
  discard pstmt.dbConn.writer[].recv()
  self.freeConn.incl pstmt.dbConn

proc reset*(self: DBPool, pstmt: DBSqlPrepared): Future[void] {.async.} =
  pstmt.dbConn.reader[].send(DBStmt(met:InterfaceKind.reset,kind:StmtKind.binary,pstmt:pstmt.pstmt))
  discard pstmt.dbConn.writer[].recv()
