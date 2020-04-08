import unittest

from all_constants import SITE_MAP
from all_section import AllSection


class AllsectionTest(unittest.TestCase):
    allsection = AllSection(SITE_MAP)
    list_urls = allsection.get_sitemap(SITE_MAP)

    def get_site_map_test(self):
        self.assertRaises(self.allsection.get_sitemap(SITE_MAP))

    def check_sitemap_test(self):
        self.assertRaises(self.allsection.check_sitemap())


class ClasterizationTest(unittest.TestCase):
    def get_match_test(self):
        pass
