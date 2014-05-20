# -*- coding: utf-8 -*-
#
# тест test_offset_and_chunksize
offset_and_chunksize_types_list = [("NULL", "NULL"),
                                   ("NULL", "DATA_SIZE"),
                                   ("NULL", "MIDDLE"),
                                   ("NULL", "OVER_SIZE"),
                                   ("MIDDLE", "NULL"),
                                   ("MIDDLE", "DATA_SIZE"),
                                   ("MIDDLE", "MIDDLE"),
                                   ("MIDDLE", "OVER_SIZE"),
                                   ("OVER_BOUNDARY", "NULL"),
                                   ("OVER_BOUNDARY", "DATA_SIZE"),
                                   ("OVER_BOUNDARY", "MIDDLE"),
                                   ("OVER_BOUNDARY", "OVER_SIZE")]
# тест test_read_with_size
size_type_list = ["NULL", "DATA_SIZE", "PART", "OVER_SIZE"]
# тест test_read_with_offset_positive
offset_type_positive_list = ["NULL", "MIDDLE"]
# тест test_read_with_offset_negative
offset_type_negative_list = ["OVER_BOUNDARY"]
# тест test_read_with_offset_and_size_positive
offset_and_size_types_positive_list = [("NULL", "NULL"),
                                       ("NULL", "DATA_SIZE"),
                                       ("NULL", "PART"),
                                       ("NULL", "OVER_SIZE"),
                                       ("MIDDLE", "NULL"),
                                       ("MIDDLE", "DATA_SIZE"),
                                       ("MIDDLE", "PART_DEPEND_ON_OFFSET_VALID"),
                                       ("MIDDLE", "PART_DEPEND_ON_OFFSET_INVALID"),
                                       ("MIDDLE", "OVER_SIZE")]
# тест test_read_with_offset_and_size_negative
offset_and_size_types_negative_list = [("OVER_BOUNDARY", "NULL"),
                                       ("OVER_BOUNDARY", "DATA_SIZE"),
                                       ("OVER_BOUNDARY", "PART"),
                                       ("OVER_BOUNDARY", "OVER_SIZE")]
# тест test_offset
write_offset_type_and_overriding_list = [("BEGINNING", False),
                                         ("BEGINNING", True),
                                         ("MIDDLE", False),
                                         ("MIDDLE", True),
                                         ("END", True),
                                         ("OVER_BOUNDARY", False),
                                         ("APPENDING", False)]
