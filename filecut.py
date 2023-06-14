from abc import ABC, abstractmethod

import pandas as pd

DEFAULT_PAGE_NUMBER_PER_TABLE = 5 * 10 ** 5


class FileChain(ABC):

    @abstractmethod
    def handler(self, src):
        pass

    @abstractmethod
    def set_handle(self):
        pass


class AbstractFileChain(FileChain):
    _next_handle: FileChain = None

    def handler(self, src):
        if self._next_handle:
            return self._next_handle.handler()
        return False

    def set_handle(self, handle: FileChain):
        self._next_handle = handle
        return handle


class XlsxFileHandle(AbstractFileChain):
    def handler(self, src):
        if not str(src).endswith('.xlsx'):
            if self._next_handle:
             return self._next_handle.handler(src)
            return False
        table = pd.ExcelFile(src)
        files = str(src).rsplit('.',maxsplit=2)
        # if len(files) != 2:
        #     raise 'invalid the fileName'
        for nums in len(table):
            df = pd.read_excel(table, sheet_name=nums)
            startIndex = 0
            maxLength = len(df)
            endIndex = startIndex
            step = 0
            while True:
                startIndex = DEFAULT_PAGE_NUMBER_PER_TABLE * step
                endIndex = min(DEFAULT_PAGE_NUMBER_PER_TABLE * (step + 1), maxLength)
                if startIndex >= maxLength:
                    break
                subDF = df.iloc[startIndex:endIndex]
                fileNameOutput = "{}_第{}工作表_{}".format(files[0], nums, step + files[1])
                subDF.to_excel(fileNameOutput)
                step = step + 1
        return True

class CSVFileHandle(AbstractFileChain):
    def handler(self, src):
        if not str(src).endswith('.csv'):
            if self._next_handle:
                return self._next_handle.handler(src)
            return False
        files =str(src).rsplit('.',2)
        df=pd.read_csv(src)
        startIndex = 0
        endIndex =startIndex
        step = 0
        while True:
            startIndex = step*DEFAULT_PAGE_NUMBER_PER_TABLE
            endIndex =min((step+1)*DEFAULT_PAGE_NUMBER_PER_TABLE,len(df))
            if startIndex >= len(df):
                break
            subDF=df.iloc[startIndex,endIndex]
            fileNameOutput = "{}_{}".format(files[0], step + files[1])
            subDF.to_csv(fileNameOutput)
            step=step+1
        return True
    

def CutFile(src):
    try:
        filechain = FileChain()
        filechain.set_handle(XlsxFileHandle()).set_handle(CSVFileHandle())
        result = filechain.handler(src)
        if result is None or not result:
            raise '处理失败'
    except Exception as e:
        print(e)
