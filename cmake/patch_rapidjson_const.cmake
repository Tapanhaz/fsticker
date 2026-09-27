# Removes RapidJSON's pre-2016 broken copy-assignment operator on
# GenericStringRef (Tencent/rapidjson#718): it assigns to a const
# member, which GCC13+/Clang reject under -std=c++20. Idempotent —
# safe to run against an already-patched or future-fixed header.
if(NOT DEFINED RAPIDJSON_HEADER)
    message(FATAL_ERROR "RAPIDJSON_HEADER not set")
endif()

file(READ "${RAPIDJSON_HEADER}" _rj_content)

set(_rj_broken_line
    "    GenericStringRef& operator=(const GenericStringRef& rhs) { s = rhs.s; length = rhs.length; }\n")

string(FIND "${_rj_content}" "${_rj_broken_line}" _rj_pos)
if(_rj_pos EQUAL -1)
    message(STATUS "[RapidJSON] const-member patch: broken operator not found -- skipping")
else()
    string(REPLACE "${_rj_broken_line}" "" _rj_content "${_rj_content}")
    file(WRITE "${RAPIDJSON_HEADER}" "${_rj_content}")
    message(STATUS "[RapidJSON] const-member patch applied to ${RAPIDJSON_HEADER}")
endif()