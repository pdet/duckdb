add_subdirectory(union)

add_library_unity(
  duckdb_func_cast
  OBJECT
  array_casts.cpp
  blob_cast.cpp
  bit_cast.cpp
  cast_function_set.cpp
  decimal_cast.cpp
  default_casts.cpp
  enum_casts.cpp
  list_casts.cpp
  map_cast.cpp
  numeric_casts.cpp
  pointer_cast.cpp
  string_cast.cpp
  struct_cast.cpp
  time_casts.cpp
  union_casts.cpp
  uuid_casts.cpp
  varint_casts.cpp
  nested_to_varchar_cast.cpp
  vector_cast_helpers.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_cast>
    PARENT_SCOPE)
