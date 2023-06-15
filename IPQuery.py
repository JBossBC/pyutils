import array
import copy
import os.path
import threading
import time
from abc import ABC, abstractmethod

import pandas as pd
import requests

DEFAULT_MAX_RETRY_TIMES = 3
DEFAULT_MAX_WORKS_NUMBER = 50
DEFAULT_MAX_SLEEP_TIMES = 2


def requestInterface():
    request = requests.Request(method='GET', url="https://api.jingjiequ.com/tools/ips",
                               headers={'Origin': "https://jingjiequ.com", "Referer": 'https://jingjiequ.com/'})

    return request


def defaultRequestChange(slices: pd.DataFrame, rawRequest: requests.Request):
    dataset = [slices.iloc[i] for i in range(len(slices.iloc))]
    sep = ','
    ## maybe cant use deepcopy
    newRequest = copy.deepcopy(rawRequest)
    newRequest.url = newRequest.url + "/" + sep.join(dataset)
    return newRequest


def defaultResponseSave(rawResponse: requests.Response):
    jsonData = rawResponse.json()
    returnData = []
    for i in range(len(jsonData)):
        tempData = jsonData[0]
        returnData.append(tempData["addresses"]['address'].decode('utf-8'))

    return returnData


def IP_Query(src, target):
    controller = Controller(src, target)
    controller.Run(request=requestInterface(), requestChange=defaultRequestChange,responseSave=defaultResponseSave,start=0,step=50 )


class Controller:
    works: []
    source: pd.DataFrame = None
    output: pd.DataFrame = None
    targetFile: None
    step: int = 0
    start: int = 0
    end: int = 0
    inputIndex: int = -1
    outputIndex: int = -1
    mutex: threading.Lock = None
    maxRetryTimes: int = DEFAULT_MAX_RETRY_TIMES
    raw_request: requests.Request = None
    maxWorksNumber: int = DEFAULT_MAX_WORKS_NUMBER
    maxSleepTimes = DEFAULT_MAX_SLEEP_TIMES
    responseSave: None
    failedWorks: []

    def __init__(self, src, target):
        chain = createDefaultInputChain()
        self.mutex = threading.Lock()
        try:
            self.source = handleInput(chain, src)
            self.output = handleOutput(chain, target)
            self.targetFile = target
            self.end = len(self.source)
            curColumns = self.source.columns
            for index in range(curColumns):
                if str(curColumns[index]).lower().find('ip') == -1:
                    continue
                self.inputIndex = index
                resultName = self.source.columns[self.inputIndex] + "IP解析"
                self.output[resultName] = None
                self.outputIndex = self.output.columns.get_loc(resultName)
                break
        except Exception as e:
            print(e)

    # request: the raw request can't contain the ip data requestChange: add the ip method (the input params must have
    # two  the arraySlice and the origin request,the output params is the eventual request)
    def Run(self, request, requestChange, responseSave, start, step):
        try:
            self.raw_request = request
            self.step = step
            self.start = start
            self.responseSave = responseSave
            self.output = None
            timeBegin = 0.002  # 2ms
            while self.start < self.end:
                while len(self.works) >= self.maxWorksNumber:
                    time.sleep(timeBegin)
                    timeBegin = timeBegin * 2
                    for i in range(self.works.__len__()):
                        worker = self.works[i]
                        if worker.is_alive():
                            continue
                        if worker.state != 1:
                            if worker.state == 0:
                                raise 'concurrent error'
                            self.failedWorks.append(worker)
                        self.works.remove(worker)
                if self.start >= self.end:
                    break
                timeBegin = 0.002
                nextQuery = min(self.start + self.step, self.end)
                realRequest = requestChange(request, self.source.iloc[self.start, nextQuery][self.inputIndex])
                newWork = work(realRequest, self)
                work.start(self.start, nextQuery)
                self.works.append(newWork)
                self.start = nextQuery
        except Exception as e:
            print(e)
        finally:
            self.finalizer()

    def finalizer(self):
        try:
            for i in range(self.failedWorks.__len__()):
                worker = self.failedWorks[i]
                worker.run()
                if not worker.is_alive() and worker.state == 1:
                    self.failedWorks.remove(worker)

            # reported the error for this running
        except Exception as e:
            print(e)
        finally:
            outputChain = createDefaultOutputChain()
            outputChain.set_dataset(self.output)
            outputChain.handle(self.targetFile)
        for i in range(self.failedWorks.__len__()):
            print("错误报告: 从", self.failedWorks[i].startIndex + 1, "行------------------>",
                  self.failedWorks[i].endIndex + 1 + "行")


def handleOutput(chain, target):
    if not os.path.exists(target):
        with open(target, 'a+', encoding='utf8'):
            pass
    return chain.handle(target)


def handleInput(chain, src):
    return chain.handle(src)


class ChainInterface(ABC):
    @abstractmethod
    def set_handle(self):
        pass

    @abstractmethod
    def handle(self, src):
        pass


class AbstractOutputChain:
    __next__chain: ChainInterface = None
    dataset: pd.DataFrame = None

    def set_dataset(self, dataset: pd.DataFrame):
        self.dataset = dataset

    def set_handle(self, handle: ChainInterface):
        self.__next__chain = handle
        return handle

    def handle(self, target):
        if self.__next__chain:
            return self.__next__chain.handle(target)
        return None


class XlsxOutput(AbstractOutputChain):
    def handle(self, target):
        if str(target).endswith('xlsx'):
            return self.dataset.to_excel(target)
        if self.__next__chain:
            return self.__next__chain.handle(target)
        return None


class CsvOutput(AbstractOutputChain):
    def handle(self, target):
        if str(target).endswith('csv'):
            return self.dataset.to_csv(target)
        if self.__next__chain:
            return self.__next__chain.handle(target)
        return None


class AbstractInputChain:
    __next__chain: ChainInterface = None

    def set_handle(self, handle: ChainInterface):
        self.__next__chain = handle
        return handle

    def handle(self, src):
        if self.__next__chain:
            return self.__next__chain.handle()
        return None


class XlsxInput(AbstractInputChain):
    def handle(self, src):
        if str(src).endswith('xlsx'):
            return pd.read_excel(src)
        if self.__next__chain:
            return self.__next__chain.handle(src)
        return None


class CsvInput(AbstractInputChain):
    def handle(self, src):
        if str(src).endswith('csv'):
            return pd.read_csv(src)
        if self.__next__chain:
            return self.__next__chain.handle(src)
        return None


def createDefaultInputChain():
    chain = XlsxInput()
    chain.set_handle(CsvInput())
    return chain


def createDefaultOutputChain():
    chain = XlsxOutput()
    chain.set_handle(CsvOutput())
    return chain


class work(threading.Thread):
    baseRequest: requests.Request = None
    responseFunc: None
    state: int = 0
    retryTimes: int = 0
    center: Controller = None
    result: None
    startIndex: int = None
    endIndex: int = None

    def start(self, start, end) -> None:
        self.startIndex = start
        self.endIndex = end

    def run(self):
        while self.retryTimes < self.center.maxRetryTimes:
            try:
                response = requests.request(self.baseRequest.method, self.baseRequest.url, self.baseRequest.params,
                                            self.baseRequest.data, self.baseRequest.headers, self.baseRequest.cookies,
                                            self.baseRequest.files, self.baseRequest.auth, json=self.baseRequest.json)
                if response.status_code == 200:
                    self.result = self.center.responseSave(response)
                    self.center.mutex.locked()
                    self.center.output.iloc[self.startIndex, self.center.outputIndex] = self.result
                    self.center.mutex.release()
                    self.state = 1
                    return
                print(response.status_code + "\r\n")
                print(response.text)
            except Exception as e:
                print(e)
            finally:
                time.sleep(2)
                self.retryTimes = self.retryTimes + 1
            self.state = 2

    def __init__(self, request, center):
        self.baseRequest = request
        self.center = center
