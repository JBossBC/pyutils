# pyutils


## CutFile

### 拆分文件大小，目前仅仅支持xlsx,csv,后续扩展


## htmlAnalysis

### 将html页面的有效数据提取出来，不同于etree，只需要关注数据位置即可


## IPQuery

### IP查询: 搜索表格中有关的IP数据，自动查询其归属地,如果target为src,则在表格后面追加归属地信息,如果为target不为src,则重新生成一个关于IP和IP归属地的表格

> 开启多线程,减小worker的粒度，增强了性能，同时对错误请求生成错误报告，便于检索和排查，基于FileCut对大文件做自动分割



> benchTest: 4w IP per minute