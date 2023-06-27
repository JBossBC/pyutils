import unittest

import IPQuery
import cProfile
import pstats


class IPQueryCase(unittest.TestCase):
    def test_IPQuery(self):
        IPQuery.IP_QueryForNewFile(r'D:\pyutils\data\后40万.xlsx', r'D:\pyutils\data\后40万.xlsx')
        self.assertEqual(True, True)  # add assertion here


if __name__ == '__main__':
    unittest.main()
    # cProfile.run("unittest.main()", "profile_stats")
    # stats = pstats.Stats('profile_stats')
    #
    # # 将性能统计信息保存到文本文件
    # stats.dump_stats('profile_stats.txt')
