add_library_unity(
  test_common
  OBJECT
  test_cast.cpp
  test_checksum.cpp
  test_file_system.cpp
  test_hyperlog.cpp
  test_utf.cpp
  test_string_util.cpp) # test_serializer.cpp
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_common>
    PARENT_SCOPE)
