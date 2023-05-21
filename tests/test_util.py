import pytest
from ndf.util import *

today = datetime.today()
next_BMF = today + relativedelta(months=+1)
next_BMF = get_second_business_day(next_BMF.month, next_BMF.year)
next_BMF = next_BMF.strftime('%Y%m%d')  # "20230602"


@pytest.mark.parametrize("test_input,expected",
                         [("", next_BMF), ("20191229", "20200103"), ("20200101", "20200103"), ("20200201", "20200204"), ("20200301", "20200303"),
                          ("20200401", "20200402"), ("20200501", "20200505"), ("20200601", "20200602"),
                          ("20200701", "20200702"), ("20200801", "20200804"), ("20200810", "20200902"), ("20200901", "20200902"),
                          ("20201001", "20201002"), ("20201101", "20201103"), ("20201201", "20201202")])
def test_get_BMF_date(test_input, expected):
    test_input = datetime.strptime(test_input, '%Y%m%d') if test_input else None
    expected = datetime.strptime(expected, '%Y%m%d')
    assert get_BMF_date(test_input) == expected


@pytest.mark.parametrize("test_input,expected",
                         [("20200101", "20200103"), ("20200201", "20200204"), ("20200301", "20200303"),
                          ("20200401", "20200402"), ("20200501", "20200505"), ("20200601", "20200602"),
                          ("20200701", "20200702"), ("20200801", "20200804"), ("20200901", "20200902"),
                          ("20201001", "20201002"), ("20201101", "20201103"), ("20201201", "20201202")])
def test_get_second_business_day(test_input, expected):
    test_input = datetime.strptime(test_input, '%Y%m%d')
    expected = datetime.strptime(expected, '%Y%m%d')
    assert get_second_business_day(test_input.month, test_input.year) == expected
