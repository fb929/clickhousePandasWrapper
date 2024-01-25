import logging
import re
import inspect
import pandas as pd
import clickhouse_driver

columnTypeMap = {
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
    def __init__(self, host='127.0.0.1', port=9000, db='default', columnTypeMap=columnTypeMap, logLevel=None):
        self.logger = logging.getLogger(self.__class__.__name__)
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

        # create clickhouse client connect
        self.ch = clickhouse_driver.Client(
            host=self.host,
            port=self.port,
        )
        self.logger.debug(f"created clickhouse client connect: host={self.host}, port={self.port}, db={self.db}")
        # create database is not exists
        try:
            self.ch.execute(f"CREATE DATABASE IF NOT EXISTS {self.db}")
        except Exception as e:
            raise Exception(f"failed create database={self.db} in clickhouse: host={self.host}, port={self.port}, error='{str(e)}'")

    def pandasToClickhouseType(self, columnName, pandasType):
        """
        matching data type from pandas to clickhouse
        """

        defName = inspect.stack()[0][3]
        mapping = {
            'int32': 'Int32',
            'int64': 'Int64',
            'float32': 'Float32',
            'float64': 'Float64',
            'object': 'String',
        }
        if columnName in self.columnTypeMap:
            # define type by column name
            return self.columnTypeMap.get(columnName)
        else:
            # define type by pandas types
            return mapping.get(str(pandasType), 'String')

    def generateCreateTableQuery(self, df, db, table, partitionBy, orderBy):
        """
        generate create table query
        """

        defName = inspect.stack()[0][3]

        self.logger.debug(f"{defName}: dtypes={df.dtypes}")
        self.logger.debug(f"{defName}: df.sample='{df.sample(n=5)}'")
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
ENGINE MergeTree
PARTITION BY ({partitionBy})
ORDER BY {orderBy}
SETTINGS index_granularity = 8192
"""
        self.logger.info(f"{defName}: create table sql='{sql}'")
        return sql

    def generateAlterQuery(self, df, table, db, partitionBy, cleanDataWhereColumns=list()):
        """
        generate alter table query
        """

        defName = inspect.stack()[0][3]
        # get date range
        try:
            dateFrom = str(min(df[partitionBy]))
            dateTo = str(max(df[partitionBy]))
        except Exception as e:
            self.logger.error(f"{defName}: failed get max and min for column={partitionBy} in df, error={str(e)}, df.sample='{df.sample(n=5)}'")
            return False

        # basic sql for clean data
        alterTable = f"ALTER TABLE {db}.{table} DELETE WHERE {partitionBy} BETWEEN '{dateFrom}' AND '{dateTo}'"

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
                    self.logger.error(f"{defName}: values for column='{cleanColumnKey}' values is not unique, cleanColumnValue='{cleanColumnValue}, df.sample='{df.sample(n=5)}'")
                    return False
                cleanColumn[cleanColumnName] = cleanColumnValue[0]
                # expand clean data sql
                alterTable = f"{alterTable} AND `{cleanColumnName}` = '{cleanColumnValue[0]}'"
            if not cleanColumn:
                self.logger.error(f"{defName}: failed get values for columns '{cleanDataWhereColumns}', columnNames={df.columns.values.tolist()}, df.sample='{df.sample(n=5)}'")
                return False
        # }}

        return alterTable

    def insertDataInClickhouse(self, df, table, db=None, cleanDataInDateRange=True, cleanDataWhereColumns=list(), partitionBy='date', orderBy='date'):
        """
        insert dataframe in clickhouse
        cleanDataInDateRange - delete data in date range before insert
        partitionBy - column name for partitioning
        orderBy - column name(s) for primary keys
        """

        defName = inspect.stack()[0][3]
        # default args {{
        if db == None:
            db = self.db
        # }}

        # check type cleanDataWhereColumns
        if not isinstance(cleanDataWhereColumns, list):
            cleanDataWhereColumns = [cleanDataWhereColumns]

        # check table exists
        try:
            tables = self.ch.execute(f'SHOW TABLES FROM {db}')
        except Exception as e:
            self.logger.error(f"{defName}: failed execute in clickhouse: host={self.host}, port={self.port}, db={db}, table={table}, error='{str(e)}'")
            return False
        tableExists = any(tbl[0] == table for tbl in tables)
        if tableExists:
            # clean exists data in table {{
            if cleanDataInDateRange:
                alterTable = self.generateAlterQuery(df,table,db,partitionBy,cleanDataWhereColumns)
                # clean data
                self.logger.info(alterTable)
                try:
                    self.ch.execute(alterTable)
                except Exception as e:
                    self.logger.error(f"{defName}: failed execute in clickhouse: host={self.host}, port={self.port}, db={db}, table={table}, error='{str(e)}'")
                    return False
            # }}
        else:
            # create table if not exists
            query = self.generateCreateTableQuery(df,db,table,partitionBy,orderBy)
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
        self.logger.info(insertString)
        try:
            self.ch.insert_dataframe(
                insertString,
                df,
                settings={ "use_numpy": True },
            )
        except Exception as e:
            self.logger.error(f"{defName}: failed insert_dataframe in clickhouse: host={self.host}, port={self.port}, db={db}, table={table}, error='{str(e)}'")
            return False
        # }}

        return True
