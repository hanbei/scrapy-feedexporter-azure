import os
import unittest

from io import BytesIO
from scrapy import Spider
from scrapy.extensions.feedexport import IFeedStorage
from twisted.internet import defer
from zope.interface.verify import verifyObject


class ADLSFeedStorageTest(unittest.TestCase):
    pass