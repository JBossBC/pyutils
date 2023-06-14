import csv
import math
import os.path
import re

from lxml import etree


class outputParams:
    def __init__(self, targetFile, targetHeader):
        self.targetFile = targetFile
        if os.path.exists(targetFile):
            raise "输出文件已经存在"
        pointIndex = -1
        for index in reversed(range(len(str(targetFile)))):
            if targetFile[index] == '.':
                pointIndex = index
                break
        if pointIndex == -1:
            raise "cant find the file type"
        self.fileName = targetFile[:pointIndex]
        self.fileType = targetFile[pointIndex + 1:]
        self.targetHeader = targetHeader


class inputParams:
    # the fileterFiles should be dict
    def __init__(self, file, filterFiles):
        if not os.path.exists(file):
            raise "未找到该文件"
        self.file = file
        if not isinstance(filterFiles, set) and filterFiles is not None:
            raise "the filterFiles should be dict"
        self.filterFiles = filterFiles
        if os.path.isdir(file):
            self.isDir = True
        else:
            self.isDir = False

    def isFilterFile(self, file):
        if self.filterFiles is None:
            return False
        for out in range(self.filterFiles):
            if str(out) == str(file):
                return True

        return False


'''
  handle:
     tablePath: 一行数据的开始元素
     unitSelect: 一列的数据选择(如果传入字符串类型，则判定为每一行中每一列数据都用相同的规则)
     pageBegin: 一个文件中真实数据从多少个元素开始
     pageNumber: 一个文件中存在多少个数据行
     pageOffset: 数据行的偏移量
'''
MAX_CSVROWS = 1048576

DEFAULT_EXTRAFILENAME = "2"


def isNumber(s):
    pattern = r'^[-+]?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?$'
    return bool(re.match(pattern, s))


def _sortByNumber(item):
    return int(item)


class handle:
    data = []
    numberSort = False

    def __init__(self, tableXPath, unitSelect, pageNumber, pageBegin, pageOffset):
        if isinstance(unitSelect, str):
            self.equalUnit = True
        else:
            self.equalUnit = False
        if pageBegin < 0:
            raise "pageBegin cant less than zero"
        self.unitSelect = unitSelect
        self.tableXPath = tableXPath
        self.pageNumber = pageNumber
        self.pageBegin = pageBegin
        self.pageOffset = pageOffset

    def wash(self, inputData, outputData):
        if not isinstance(inputData, inputParams) or not isinstance(outputData, outputParams):
            raise "input params cant correspond,should be input and output type"

        if not inputData.isDir:
            self.handleFile(inputData.file)
        ## cant support the Loop traversal
        try:
            for root, dirs, files in os.walk(inputData.file):
                if not self.numberSort and files is not None and isNumber(files[0]):
                    self.numberSort = True
                files = files if not self.numberSort else sorted(files, key=_sortByNumber)
                for file in files:
                    if inputData.filterFiles is not None and str(file) in inputData.filterFiles:
                        continue
                    self._handleFile(str(inputData.file + "\\" + file))
        except Exception as e:
            print(str("运行到{}文件时出现异常:{}".format(file, e.__str__())) + "\n")

        self._finalizerData(outputData)

    def _finalizerData(self, outputData):
        if self.data is None:
            return
        with open(outputData.targetFile, "w+", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            rowsNumber = 1
            w.writerow(outputData.targetHeader)
            for index in range(len(self.data)):
                w.writerow(self.data[index])
                rowsNumber = rowsNumber + 1
                if rowsNumber >= MAX_CSVROWS:
                    if not f.closed:
                        f.close()
                    f = open(
                        str(outputData.fileName) + "({}).".format(DEFAULT_EXTRAFILENAME) + str(outputData.fileType),
                        encoding="utf-8", newline="")
                    w = csv.writer(f)
                    w.writerow(outputData.targetHeader)
                    rowsNumber = 1
                    DEFAULT_EXTRAFILENAME = DEFAULT_EXTRAFILENAME + 1

    def _handleFile(self, file):
        with open(r'{}'.format(file), 'r', encoding="utf-8") as f:
            res = f.read()
        if os.stat(file).st_size == 0:
            print("{} 文件为空,请检查\n".format(file))
            return
        html = etree.HTML(res)
        lineLength = len(html.xpath(self.tableXPath))
        if lineLength >= self.pageNumber + self.pageOffset * self.pageNumber:
            lineLength = self.pageNumber
        elif lineLength < self.pageNumber + self.pageOffset * self.pageNumber:
            lineLength = int(math.floor(float(lineLength / (self.pageOffset + 1))))
        times = 0

        if self.equalUnit:
            times = 1
        else:
            times = len(self.unitSelect)
        for row in range(self.pageBegin, self.pageBegin + (self.pageOffset + 1) * lineLength, self.pageOffset + 1):
            result = []
            for i in range(times):
                try:
                    if self.unitSelect == "":
                        continue
                    info = html.xpath(self.tableXPath + "[{}]".format(row) + "/" + self.unitSelect[i])[
                        0].strip()
                    if info is None or info == "":
                        info = "-"
                    result.append(info)
                except IndexError:
                    info = "-"
                    result.append(info)
                    pass
                except Exception as e:
                    raise "程序异常:{}".format(e.__str__())

            self.data.append(result)
