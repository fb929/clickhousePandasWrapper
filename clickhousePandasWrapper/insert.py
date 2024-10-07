import logging
import re
import inspect
import pandas as pd
import clickhouse_driver
import json

pandasTypeToClickhouseTypeMap = {
    'uint8': 'UInt8',
    'uint16': 'UInt16',
    'uint32': 'UInt32',
    'uint64': 'UInt64',
    'int8': 'Int8',
    'int16': 'Int16',
    'int32': 'Int32',
    'int64': 'Int64',
    'float16': 'Float32',
    'float32': 'Float32',
    'float64': 'Float64',
    'float128': 'Float64',
    'object': 'String',
    'datetime64[D]': 'Date',
    'datetime64[ns]': 'DateTime',
    'bool': 'Bool',
}
clickhouseTypeToPandasTypeMap = {
    'UInt8': 'uint8',
    'UInt16': 'uint16',
    'UInt32': 'uint32',
    'UInt64': 'uint64',
    'Int8': 'int8',
    'Int16': 'int16',
    'Int32': 'int32',
    'Int64': 'int64',
    'Float32': 'float32',
    'Float64': 'float64',
    'String': 'object',
    'Date': 'datetime64[D]',
    'DateTime': 'datetime64[ns]',
    'Bool': 'bool',
}
columnNameToTypeMap = {
    'Date': 'DateTime64(3)',
    'date': 'DateTime64(3)',
}

class Insert():
    """
    init class
```
import clickhousePandasWrapper
clickhouseInserter = clickhousePandasWrapper.Insert()
```
    or
```
clickhouseInserter = clickhousePandasWrapper.Insert(host='127.0.0.1', port='9000', db='default')
```
    insert data
```
clickhouseInserter.insertDataInClickhouse(df=df,table='test')
```
    """
    def __init__(self,
        host = '127.0.0.1',
        port = 9000,
        connect_timeout = 10,
        send_receive_timeout = 300,
        sync_request_timeout = 5,
        db = 'default',
        columnTypeMap = None, # backward compatibility, new variable columnNameToTypeMap
        columnNameToTypeMap = columnNameToTypeMap,
        logLevel = None,
        pandasTypeToClickhouseTypeMap = pandasTypeToClickhouseTypeMap,
        clickhouseTypeToPandasTypeMap = clickhouseTypeToPandasTypeMap,
    ):
        self.logger = logging.getLogger(__name__)
        if logLevel:
            if re.match(r"^(warn|warning)$", logLevel, re.IGNORECASE):
                self.logger.setLevel(logging.WARNING)
            elif re.match(r"^debug$", logLevel, re.IGNORECASE):
                self.logger.setLevel(logging.DEBUG)
            else:
                self.logger.setLevel(logging.INFO)
        args = locals()
        for argName, argValue in args.items():
            if argName != 'self':
                setattr(self, argName, argValue)

        # backward compatibility
        if columnTypeMap:
            self.columnNameToTypeMap = columnTypeMap

        # create clickhouse client connect
        self.ch = clickhouse_driver.Client(
            host = self.host,
            port = self.port,
            connect_timeout = self.connect_timeout,
            send_receive_timeout = self.send_receive_timeout,
            sync_request_timeout = self.sync_request_timeout,
        )
        self.logger.debug(f"created clickhouse client connect: host={self.host}, port={self.port}, db={self.db}")
        self.createDatabase()

    def createDatabase(self,db=None):
        # default args {{
        if db == None:
            db = self.db
        # }}
        try:
            self.ch.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
        except Exception as e:
            raise Exception(f"failed create database={db} in clickhouse: host={self.host}, port={self.port}, error='{str(e)}'")

    def dfSample(self, df):
        """
        safe way to get df sample
        """

        sampleSize = min(len(df), 5)
        sampleData = df.sample(n=sampleSize)
        return sampleData

    def pandasToClickhouseType(self, columnName, pandasType):
        """
        matching data type from pandas to clickhouse
        """

        defName = inspect.stack()[0][3]
        if columnName in self.columnNameToTypeMap:
            # define type by column name
            return self.columnNameToTypeMap.get(columnName)
        else:
            # define type by pandas types
            return self.pandasTypeToClickhouseTypeMap.get(str(pandasType), 'String')

    def generateCreateTableQuery(self, df, db, table, engine = 'MergeTree', partitionBy = None, orderBy = None, primaryKey = None, settings = 'index_granularity = 8192'):
        """
        generate create table query
        """

        defName = inspect.stack()[0][3]

        self.logger.debug(f"{defName}: df.dtypes='{df.dtypes}'")
        self.logger.debug(f"{defName}: df.sample='{self.dfSample(df)}'")
        columnDefinitions = []
        columnsStr = ''
        for columnName, dtype in df.dtypes.items():
            chType = self.pandasToClickhouseType(columnName,dtype)
            columnDefinitions.append(f"{columnName} {chType}")
            if columnsStr:
                columnsStr = columnsStr + '\n'
            columnsStr = columnsStr +'    `'+ columnName +'` '+ chType +','
        sql = f"""
CREATE TABLE IF NOT EXISTS {db}.{table} (
{columnsStr}
)
ENGINE {engine}
"""
        if primaryKey:
            sql = sql + f"""
PRIMARY KEY ({primaryKey})
"""
        if partitionBy:
            sql = sql + f"""
PARTITION BY ({partitionBy})
"""
        if orderBy:
            sql = sql + f"""
ORDER BY {orderBy}
"""
        if settings:
            sql = sql + f"""
SETTINGS {settings}
"""
        self.logger.info(f"{defName}: create table sql='{sql}'")
        return sql

    def generateAlterQuery(self, df, table, db, partitionByTable, cleanDataWhereColumns=list()):
        """
        generate alter table query
        """

        defName = inspect.stack()[0][3]
        # get date range
        try:
            dateFrom = str(min(df[partitionByTable]))
            dateTo = str(max(df[partitionByTable]))
        except Exception as e:
            self.logger.error(f"{defName}: failed get max and min for column={partitionByTable} in df, error={str(e)}, df.sample='{self.dfSample(df)}'")
            return False

        # basic sql for clean data
        alterTable = f"ALTER TABLE {db}.{table} DELETE WHERE {partitionByTable} BETWEEN '{dateFrom}' AND '{dateTo}'"

        # get values for cleanDataWhereColumns {{
        if cleanDataWhereColumns:
            cleanColumn = dict()
            for cleanColumnKey in cleanDataWhereColumns:
                try:
                    cleanColumnValue = df[cleanColumnKey].unique()
                except Exception as e:
                    self.logger.warning(f"{defName}: failed get value for key='{cleanColumnKey}' from df, columnNames={df.columns.values.tolist()}, error='{str(e)}'")
                    continue
                else:
                    cleanColumnName = cleanColumnKey
                if len(cleanColumnValue) != 1:
                    self.logger.error(f"{defName}: values for column='{cleanColumnKey}' values is not unique, cleanColumnValue='{cleanColumnValue}, df.sample='{self.dfSample(df)}'")
                    return False
                cleanColumn[cleanColumnName] = cleanColumnValue[0]
                # expand clean data sql
                alterTable = f"{alterTable} AND `{cleanColumnName}` = '{cleanColumnValue[0]}'"
            if not cleanColumn:
                self.logger.error(f"{defName}: failed get values for columns '{cleanDataWhereColumns}', columnNames={df.columns.values.tolist()}, df.sample='{self.dfSample(df)}'")
                return False
        # }}

        return alterTable

    def syncTableSchema(self,df,table,db):
        """
        add columns if not exist
        """

        defName = inspect.stack()[0][3]
        self.logger.info(f"{defName}: sync table schema in clickhouse: host={self.host}, port={self.port}, db={db}, table={table}")
        try:
            columnsInfo = self.ch.execute(f"DESCRIBE TABLE {db}.{table}")
            columnNames = [column[0] for column in columnsInfo]
        except Exception as e:
            raise Exception(f"{defName}: failed get columns names from clickhouse for table: host={self.host}, port={self.port}, db={db}, table={table}")
        self.logger.debug(f"{defName}: columnNames={columnNames}")
        for columnName, dtype in df.dtypes.items():
            if columnName in columnNames:
                continue
            chType = self.pandasToClickhouseType(columnName,dtype)
            sql = f"ALTER TABLE {db}.{table} ADD COLUMN `{columnName}` {chType}"
            self.logger.debug(f"{defName}: sql='{sql}'")
            try:
                self.ch.execute(sql)
            except Exception as e:
                raise Exception(f"{defName}: failed execute in clickhouse, sql={sql}: host={self.host}, port={self.port}, db={db}, table={table}, error='{str(e)}'")
        return None

    def syncDataTypes(self,df,table,db):
        """
        Sync data types from clickhouse table to dataframe.
        When working with small-sized dataframes, it often happens that the data type changes drastically.
        For example, one dataframe might contain a float, while the next one might have None (string).
        """

        defName = inspect.stack()[0][3]
        self.logger.debug(f"{defName}: sync data types from clickhouse to df: host={self.host}, port={self.port}, db={db}, table={table}")
        query = f"DESCRIBE TABLE {db}.{table}"
        try:
            columnsInfo = self.ch.execute(query)
        except Exception as e:
            raise Exception(f"{defName}: failed execute: query='{query}' host={self.host}, port={self.port}, db={db}, table={table}")

        for columnInfo in columnsInfo:
            chColumnName = columnInfo[0]
            chColumnType = columnInfo[1]
            dfColumntType = self.clickhouseTypeToPandasTypeMap.get(chColumnType, 'object')
            if chColumnName in df.columns:
                # convert arrays to string
                if df[chColumnName].apply(lambda x: isinstance(x, list)).any():
                    self.logger.debug(f"{defName}: chColumnName={chColumnName} convert list to string")
                    try:
                        df[chColumnName] = df[chColumnName].astype("string")
                    except Exception as e:
                        self.logger.warning(f"{defName}: failed convert dataframe type for column='{chColumnName}' to type='string', error={str(e)}")

                # None not possible value for int\float types
                if dfColumntType in ['int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64', 'float16', 'float32', 'float64']:
                    df[chColumnName] = df[chColumnName].fillna(0)

                # fix astype for String
                if chColumnType == 'String':
                    try:
                        df[chColumnName] = df[chColumnName].astype("string")
                    except Exception as e:
                        self.logger.warning(f"{defName}: failed convert dataframe type for column='{chColumnName}' to type='string', error={str(e)}")

                # try convert astype for df
                try:
                    self.logger.debug(f"{defName}: set astype={dfColumntType}, chColumnName={chColumnName}, chColumnType={chColumnType}")
                    df[chColumnName] = df[chColumnName].astype(dfColumntType)
                except Exception as e:
                    raise Exception(f"{defName}: failed run astype. chColumnName='{chColumnName}', chColumnType='{chColumnType}', dfColumntType='{dfColumntType}', dfValues='{df[chColumnName].to_string()}', error='{str(e)}'")

        return df

    def insertDataInClickhouse(self,
            df,
            table,
            db=None,
            cleanDataInDateRange=True,
            cleanDataWhereColumns=list(),
            cleanAllData=False,
            partitionByTable='date',
            partitionByFunction='toYYYYMM',
            primaryKey=None,
            orderBy=None,
            retryCounter=0
        ):
        """
        insert dataframe in clickhouse
        cleanDataInDateRange - delete data in date range before insert
        partitionByTable - column name for partitioning
        orderBy - string for generate 'order by'
        """

        defName = inspect.stack()[0][3]
        # default args {{
        if db == None:
            db = self.db
        if orderBy == None:
            orderBy = partitionByTable
        # }}

        # check type cleanDataWhereColumns
        if not isinstance(cleanDataWhereColumns, list):
            cleanDataWhereColumns = [cleanDataWhereColumns]

        # check table exists {{
        try:
            tables = self.ch.execute(f'SHOW TABLES FROM {db}')
        except Exception as e:
            if e.code == 81: # UNKNOWN_DATABASE https://github.com/mymarilyn/clickhouse-driver/blob/master/clickhouse_driver/errors.py#L82C5-L82C21
                self.logger.warning(f"{defName}: database not found in clickhouse, creating: host={self.host}, port={self.port}, db={db}, table={table}'")
                self.createDatabase(db)
                # retry
                if retryCounter >= 1:
                    raise Exception(f"{defName}: failed execute in clickhouse: host={self.host}, port={self.port}, db={db}, table={table}, error='{str(e)}'")
                else:
                    return self.insertDataInClickhouse(
                        df = df,
                        table = table,
                        db = db,
                        cleanDataInDateRange = cleanDataInDateRange,
                        cleanDataWhereColumns = cleanDataWhereColumns,
                        partitionByTable = partitionByTable,
                        partitionByFunction = partitionByFunction,
                        orderBy = orderBy,
                        retryCounter = retryCounter+1,
                    )
            else:
                self.logger.error(f"{defName}: failed execute in clickhouse: host={self.host}, port={self.port}, db={db}, table={table}, error='{str(e)}'")
                return False
        # }}
        tableExists = any(tbl[0] == table for tbl in tables)
        if tableExists:
            # clean exists data in table
            if cleanDataInDateRange or cleanAllData:
                if cleanDataInDateRange:
                    sql = self.generateAlterQuery(
                        df = df,
                        table = table,
                        db = db,
                        partitionByTable = partitionByTable,
                        cleanDataWhereColumns = cleanDataWhereColumns,
                    )
                elif cleanAllData:
                    sql = f"TRUNCATE TABLE IF EXISTS {db}.{table}"
                # clean data
                self.logger.debug(sql)
                try:
                    self.ch.execute(sql)
                except Exception as e:
                    self.logger.error(f"{defName}: failed execute in clickhouse: host={self.host}, port={self.port}, db={db}, table={table}, error='{str(e)}'")
                    return False

        else:
            # create table if not exists
            if partitionByFunction and partitionByTable:
                partitionBy = partitionByFunction+'('+partitionByTable+')'
            else:
                partitionBy = None
            query = self.generateCreateTableQuery(
                df = df,
                db = db,
                table = table,
                partitionBy = partitionBy,
                orderBy = orderBy,
                primaryKey = primaryKey,
            )
            try:
                self.ch.execute(query)
            except Exception as e:
                self.logger.error(f"{defName}: failed execute in clickhouse: host={self.host}, port={self.port}, db={db}, table={table}, error='{str(e)}'")
                return False

        ## insert data {{
        columnsString = ''
        for column in df.columns.values.tolist():
            if columnsString:
                columnsString = columnsString +","
            columnsString = columnsString +"`"+ column +"`"
        insertString = 'INSERT INTO %s.%s (%s) VALUES' % (db,table,columnsString)
        self.logger.debug(insertString)

        # sync data typs from clickhouse to df
        df = self.syncDataTypes(df,table,db)

        try:
            self.ch.insert_dataframe(
                insertString,
                df,
                settings={ "use_numpy": True },
            )
        except Exception as e:
            pd.set_option('display.max_rows', None)
            pd.set_option('display.max_columns', None)
            self.logger.warning(f"{defName}: failed insert error='{str(e)}'")
            self.logger.warning(f"{defName}: df.dtypes='{df.dtypes}'")
            self.logger.warning(f"{defName}: df='{json.dumps(json.loads(df.to_json(orient='table')), indent=4)}'")
            if e.code == 16: # NO_SUCH_COLUMN_IN_TABLE https://github.com/mymarilyn/clickhouse-driver/blob/master/clickhouse_driver/errors.py#L17
                self.logger.warning(f"{defName}: schema in clickhouse table does not match df schema: host={self.host}, port={self.port}, db={db}, table={table}'")
                self.syncTableSchema(df,table,db)
                # retry
                if retryCounter >= 1:
                    raise Exception(f"{defName}: failed insert_dataframe in clickhouse: host={self.host}, port={self.port}, db={db}, table={table}, error='{str(e)}'")
                else:
                    return self.insertDataInClickhouse(
                        df = df,
                        table = table,
                        db = db,
                        cleanDataInDateRange = cleanDataInDateRange,
                        cleanDataWhereColumns = cleanDataWhereColumns,
                        partitionByTable = partitionByTable,
                        partitionByFunction = partitionByFunction,
                        orderBy = orderBy,
                        retryCounter = retryCounter+1,
                    )
            else:
                self.logger.error(f"{defName}: failed insert_dataframe in clickhouse: host={self.host}, port={self.port}, db={db}, table={table}, error='{str(e)}'")
                return False
        # }}

        return True
