# -*- coding: utf-8 -*-

import unittest

from openprocurement.litepublic.tests import tender, health, spore


def suite():
    suite = unittest.TestSuite()
    suite.addTest(tender.suite())
    suite.addTest(health.suite())
    suite.addTest(spore.suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
