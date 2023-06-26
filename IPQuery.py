import copy
import os.path
import signal
import threading
import time
from abc import ABC, abstractmethod

import numpy
import pandas as pd
import requests
from openpyxl import Workbook

import filecut

DEFAULT_WAIT_OTHERSWORKS_TIMES = 8
DEFAULT_RETRY_SESSION_TIMES = 2
DEFAULT_MAX_RETRY_TIMES = 10
DEFAULT_MAX_WORKS_NUMBER = 80
DEFAULT_MAX_SLEEP_TIMES = 2
DEFAULT_DRAIN_SLEEP_TIMES = 3
DEFAULT_ADDITIONAL_IP_NAME = 'IP'

DEFAULT_FAILED_INTERNAL = 0.5

DEFAULT_MONITOR_SLEEP_TIME = 2


def requestInterface():
    headers = {
        'Accept': 'application/prs.jjq.v1+json',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Origin': 'https://jingjiequ.com',
        'Pragma': 'no-cache',
        'Referer': 'https://jingjiequ.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
        'jjq-from': 'web',
        'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    request = requests.Request(method='GET', url="https://api.jingjiequ.com/tools/ips",
                               headers=headers)

    return request


def defaultRequestChange(slices: numpy.array, rawRequest: requests.Request):
    dataset = [slices[i] for i in range(len(slices))]
    sep = ','
    ## maybe cant use deepcopy
    newRequest = copy.deepcopy(rawRequest)
    newRequest.url = newRequest.url + "/" + sep.join(dataset)
    return newRequest


def defaultResponseSave(rawResponse: requests.Response):
    jsonData = rawResponse.json()
    returnData = []
    # print(rawResponse.text)
    try:
        for i in range(len(jsonData)):
            tempData = jsonData[i]
            # print(tempData)
            returnData.append(tempData['addresses'][0]["address"])
    except Exception as e:
        print(e, "\r\n")
        print(jsonData)
    return returnData


def IP_Query(src, target):
    controller = Controller(src, target)
    controller.Run(request=requestInterface(), requestChange=defaultRequestChange, responseSave=defaultResponseSave,
                   start=0, step=50)


class Controller:
    works: []
    source: pd.DataFrame = None
    output: pd.DataFrame
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
    additionalIndex: -1
    additionalFlag = False
    drain = False
    session: requests.Session = None
    forcedSign: bool = False
    buryingPointer: bool = False

    def __init__(self, src, target):
        chain = createDefaultInputChain()
        self.maxRetryTimes = DEFAULT_MAX_RETRY_TIMES
        self.maxSleepTimes = DEFAULT_MAX_SLEEP_TIMES
        self.maxWorksNumber = DEFAULT_MAX_WORKS_NUMBER
        self.buryingPointer = False
        self.forcedSign = False
        # start finalizer monitor threading   warning: this function is only match the single progress
        self.monitorFinalizer()
        self.mutex = threading.Lock()
        self.works = []
        self.failedWorks = []
        if src == target:
            self.additionalFlag = True
        try:
            self.source = chain.handle(src)
            self.targetFile = target
            self.end = len(self.source)
            if os.path.exists(target):
                self.output = chain.handle(target)
            else:
                self.output = pd.DataFrame(index=range(self.end), columns=[DEFAULT_ADDITIONAL_IP_NAME])

            # if not os.path.exists(target):
            #     emptyWorkBook = Workbook()
            #     emptyWorkBook.save(target)
            # with open(target, 'x', encoding='utf8'):
            # pass
            curColumns = self.source.columns
            # print(self.source.loc[0])
            if not self.additionalFlag:
                self.output[DEFAULT_ADDITIONAL_IP_NAME] = pd.Series(dtype='object')
                self.additionalIndex = self.output.columns.get_loc(DEFAULT_ADDITIONAL_IP_NAME)
            for index in range(len(curColumns)):
                if str(curColumns[index]).lower().find('ip') == -1:
                    continue
                self.inputIndex = index
                resultName = self.source.columns[self.inputIndex] + "解析"
                self.output[resultName] = pd.Series(dtype='object', index=range(self.end))
                self.outputIndex = self.output.columns.get_loc(resultName)
                break

        except Exception as e:
            self.forcedSign = True
            print(e)

    # request: the raw request can't contain the ip data requestChange: add the ip method (the input params must have
    # two  the arraySlice and the origin request,the output params is the eventual request)
    def Run(self, request, requestChange, responseSave, start, step):
        try:
            self.session = requests.Session()
            self.raw_request = request
            self.step = step
            self.start = start
            self.responseSave = responseSave
            timeBegin = 0.05  # 50ms
            while self.start < self.end:
                while self.works.__len__() >= self.maxWorksNumber:
                    if timeBegin > DEFAULT_MAX_SLEEP_TIMES:
                        timeBegin = DEFAULT_MAX_SLEEP_TIMES
                    time.sleep(timeBegin)
                    timeBegin = timeBegin * 2
                    try:
                        self.mutex.acquire()
                        # print("正在清理旧的worker")
                        sumNum = 0
                        needToClean = []
                        for i in range(self.works.__len__()):
                            # concurrent take works.__len__() is wrong
                            # if i >= self.works.__len__():
                            #     break
                            worker = self.works[i]
                            if worker.is_alive():
                                continue
                            # if worker.state != 1:
                            if worker.state == 0:
                                # raise 'concurrent error'
                                # needToClean.append(worker)
                                self.failedWorks.append(worker)
                            sumNum = sumNum + 1

                            def removeWorker():
                                # clean the worker pointer to GC
                                worker.center = None
                                needToClean.append(worker)

                            removeWorker()
                    finally:
                        for i in range(len(needToClean)):
                            self.works.remove(needToClean[i])
                        if sumNum != 0 and self.buryingPointer:
                            print("此轮总共清理worker数目:", sumNum)
                        self.mutex.release()
                if self.start >= self.end:
                    break
                timeBegin = 0.05
                nextQuery = min(self.start + self.step, self.end)
                realRequest = requestChange(self.source.iloc[:, self.inputIndex].values[self.start: nextQuery], request)
                newWork = work(realRequest, self.source.iloc[:, self.inputIndex].values[self.start: nextQuery], self,
                               self.start, nextQuery)
                newWork.start()
                self.works.append(newWork)
                self.start = nextQuery
                if self.buryingPointer:
                    print("当前执行到", self.start)
        except Exception as e:
            print(e)
            self.forcedSign = True
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
            if filecut.exceedMaxNumber(self.output):
                filecut.CutFile(self.targetFile)
            for i in range(self.failedWorks.__len__()):
                self.failedWorks[i].run()
                if self.failedWorks[i].state is not 1:
                    print("错误报告: 从", str(int(self.failedWorks[i].startIndex) + 1), "行------------------>",
                          str(self.failedWorks[i].endIndex + 1), "行")

    def monitorFinalizer(self):
        # pass
        # will make main threading block
        def monitorFunc():
            while True:
                if self.forcedSign:
                    while not self.drain:
                        for i in range(len(self.works)):
                            temp = self.works[i]
                            if temp.state == 0:
                                break
                            if i == len(self.works) - 1:
                                time.sleep(DEFAULT_WAIT_OTHERSWORKS_TIMES)
                                self.drain = True
                                break
                        if not self.drain:
                            time.sleep(DEFAULT_DRAIN_SLEEP_TIMES)
                    # when the forced sign begin,should drain the all resource
                    self.finalizer()
                    currentID = os.getpid()
                    os.kill(currentID, signal.SIGTERM)
                    return
                time.sleep(DEFAULT_MONITOR_SLEEP_TIME)

        threading.Thread(target=monitorFunc).start()


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
            return pd.read_excel(src, engine='openpyxl')
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
    baseData: None

    # def start(self):
    #     super().start()

    def run(self):
        while self.retryTimes < self.center.maxRetryTimes:
            try:
                # response = requests.request(self.baseRequest.method, self.baseRequest.url, self.baseRequest.params,
                #                             self.baseRequest.data, self.baseRequest.headers, self.baseRequest.cookies,
                #                             self.baseRequest.files, self.baseRequest.auth, json=self.baseRequest.json)
                # multi session can appear the connection is too much to take system crash
                # session = requests.Session()
                # requests.
                prepare_request = self.center.session.prepare_request(self.baseRequest)
                response = self.center.session.send(prepare_request)
                if response.status_code == 200:
                    self.result = self.center.responseSave(response)
                    try:
                        self.center.mutex.acquire()
                        if self.center.additionalFlag:
                            temp = pd.DataFrame({self.center.output.columns[self.center.outputIndex]: self.result})
                            for i in range(self.startIndex, self.endIndex, 1):
                                self.center.output.loc[i,
                                                       [self.center.output.columns[self.center.outputIndex]]] = \
                                    temp.loc[i - self.startIndex]
                        else:
                            temp = pd.DataFrame({self.center.output.columns[self.center.additionalIndex]: [
                                self.baseData[i] for i in range(len(self.baseData))],
                                self.center.output.columns[self.center.outputIndex]: self.result})
                            for i in range(self.startIndex, self.endIndex, 1):
                                self.center.output.loc[
                                    i, [self.center.output.columns[self.center.additionalIndex],
                                        self.center.output.columns[self.center.outputIndex]]] = temp.loc[
                                    i - self.startIndex]
                        self.state = 1
                        return
                    except Exception as e:
                        if e is requests.ConnectionError:
                            # self.center.mutex.acquire()
                            # if not self.center.mutex.locked():
                            #     self.center.mutex.acquire()
                            self.center.session.close()
                            self.center.session = None
                            time.sleep(DEFAULT_RETRY_SESSION_TIMES)
                            self.center.session = requests.session()
                            # self.center.mutex.release()
                        print(e, "\r\n")
                        print(response.status_code, "\r\n")
                        print(response.text)
                    finally:
                        self.center.mutex.release()
            # except Exception as e:
            #     print(e)
            #     self.state = 2
            finally:
                if self.state != 1:
                    time.sleep(DEFAULT_FAILED_INTERNAL)
                self.retryTimes = self.retryTimes + 1

    def __init__(self, request, baseData, center, start, end):
        super().__init__()
        self.baseData = baseData
        self.startIndex = start
        self.endIndex = end
        self.baseRequest = request
        self.center = center
