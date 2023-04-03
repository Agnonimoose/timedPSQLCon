import psycopg2 as ps
from psycopg2.extras import execute_values
import functools, time, traceback
from threading import Thread


class DBConnect:
    def __init__(self, host='localhost', port=5432, dbname="", user="postgres", password="", timeout=5, reconnect=True):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self._lastCalled = 0
        self._timeout = timeout
        self._reconnect = reconnect

    def checkConnection(func):
        @functools.wraps(func)
        def wraped(self, *args, **kwargs):
            if (self.closed == 1) and (self._reconnect == True):
                self.connect()
                result = func(self, *args, **kwargs)
                self._lastCalled = time.time()
            elif self.closed==0:
                result = func(self, *args, **kwargs)
                self._lastCalled = time.time()
            else:
                result = False
            return result
        return wraped

    def _timeouter(self):
        try:
            while self.closed == 0:
                if (time.time() - self._lastCalled) > self._timeout:
                    self.close()
                else:
                    time.sleep(self._timeout/5)
        except Exception as e:
            print(e)
            print(str(traceback.format_exc(10)))

    def connect(self, host=None, port=None, dbname=None, user=None, password=None, retries = 0):
        if retries > 6:
            raise Exception()
        else:
            try:
                if host == None:
                    host = self.host
                if port == None:
                    port = self.port
                if dbname == None:
                    dbname = self.dbname
                if user == None:
                    user = self.user
                if password == None:
                    password = self.password
                self.con = ps.connect(host=host, port=port, dbname=dbname, user=user, password=password)
                self.cur = self.con.cursor()
                self._lastCalled = time.time()
                x = Thread(target=self._timeouter, daemon=True)
                x.start()
            except Exception as e:
                print(e)
                print(str(traceback.format_exc(10)))
                print("retryin to connect")
                time.sleep(1)
                self.connect(retries = retries + 1)

    @property
    def closed(self):
        return self.con.closed

    def close(self):
        if self.con.closed != 1:
            self.con.close()
            self._lastCalled = 0

    @checkConnection
    def commit(self):
        try:
            self.con.commit()
            return True
        except Exception as e:
            print(e)
            print(str(traceback.format_exc(10)))

    @checkConnection
    def execute(self, *args):
        try:
            self.cur.execute(*args)
            return True
        except Exception as e:
            print(e)
            print(str(traceback.format_exc(10)))

    @checkConnection
    def execute_values(self, sql, data):
        try:
            execute_values(self.cur, sql, data)
            return True
        except Exception as e:
            print(e)
            print(str(traceback.format_exc(10)))

    @checkConnection
    def fetchone(self):
        try:
            return self.cur.fetchone()
        except Exception as e:
            print(e)
            print(str(traceback.format_exc(10)))

    @checkConnection
    def fetchall(self):
        try:
            result = self.cur.fetchall()
            return result
        except Exception as e:
            print(e)
            print(str(traceback.format_exc(10)))
