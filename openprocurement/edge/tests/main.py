# -*- coding: utf-8 -*-

import unittest

from openprocurement.edge.tests import tenders, auctions, contracts, plans, health, spore


def suite():
    suite = unittest.TestSuite()
    suite.addTest(tenders.suite())
    suite.addTest(auctions.suite())
    suite.addTest(contracts.suite())
    suite.addTest(plans.suite())
    suite.addTest(health.suite())
    suite.addTest(spore.suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
