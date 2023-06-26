import unittest

import IPQuery
import cProfile
import pstats


class IPQueryCase(unittest.TestCase):
    def test_IPQuery(self):
        IPQuery.IP_Query(r'D:\pyutils\data\ip归属地-常德.xlsx', r'D:\pyutils\data\大量IP解析.xlsx')
        self.assertEqual(True, True)  # add assertion here


if __name__ == '__main__':
    cProfile.run("unittest.main()", "profile_stats")
    stats = pstats.Stats('profile_stats')

    # 将性能统计信息保存到文本文件
    stats.dump_stats('profile_stats.txt')
