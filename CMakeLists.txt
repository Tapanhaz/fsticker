cmake_minimum_required(VERSION 3.20)

project(fsticker_deps LANGUAGES C)

set(Boost_USE_STATIC_LIBS ON CACHE BOOL "" FORCE)
set(Boost_USE_STATIC_RUNTIME OFF CACHE BOOL "" FORCE)
set(FSTICKER_EXTRA_INCLUDE_DIRS "" CACHE STRING
    "Optional extra hint paths to check for rapidjson/document.h before fetching it")
 
find_path(RAPIDJSON_INCLUDE_DIR NAMES rapidjson/document.h
    HINTS ${FSTICKER_EXTRA_INCLUDE_DIRS} ENV RAPIDJSON_INCLUDE_DIR)
 
if(NOT RAPIDJSON_INCLUDE_DIR)
    message(STATUS "[RapidJSON] Not found via FSTICKER_EXTRA_INCLUDE_DIRS/env -- fetching header-only source.")
    include(FetchContent)
    FetchContent_Declare(rapidjson
        GIT_REPOSITORY https://github.com/Tencent/rapidjson.git
        GIT_TAG        v1.1.0
        GIT_SHALLOW    TRUE
        SOURCE_SUBDIR  __header_only_no_cmake__
        PATCH_COMMAND  ${CMAKE_COMMAND} -E echo "[RapidJSON] Patching GCC13+ const-member bug (Tencent/rapidjson#718)"
                COMMAND sed -i.bak
                        "/GenericStringRef& operator=(const GenericStringRef& rhs) { s = rhs.s; length = rhs.length; }/d"
                        <SOURCE_DIR>/include/rapidjson/document.h
        )
    FetchContent_MakeAvailable(rapidjson)
    set(RAPIDJSON_INCLUDE_DIR "${rapidjson_SOURCE_DIR}/include" CACHE PATH "" FORCE)
endif()
 
if(NOT EXISTS "${RAPIDJSON_INCLUDE_DIR}/rapidjson/document.h")
    message(FATAL_ERROR "RapidJSON header not found even after fetch: ${RAPIDJSON_INCLUDE_DIR}")
endif()
message(STATUS "[RapidJSON] Using ${RAPIDJSON_INCLUDE_DIR}")


set(OPENSSL_PATHS_FILE "${CMAKE_CURRENT_BINARY_DIR}/openssl_paths.txt")

if(DEFINED ENV{OPENSSL_ROOT_DIR} AND NOT OPENSSL_ROOT_DIR)
    set(OPENSSL_ROOT_DIR "$ENV{OPENSSL_ROOT_DIR}")
endif()

if(NOT OPENSSL_ROOT_DIR)
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        foreach(_pm apt-get dnf yum)
            find_program(_PM_BIN ${_pm})
            if(_PM_BIN)
                if(_pm STREQUAL "apt-get")
                    set(_pm_args install -y libssl-dev)
                else()
                    set(_pm_args install -y openssl-devel)
                endif()
                execute_process(COMMAND ${_PM_BIN} ${_pm_args}
                    RESULT_VARIABLE _pm_rc OUTPUT_QUIET ERROR_QUIET)
                if(_pm_rc EQUAL 0)
                    message(STATUS "[OpenSSL] ${_pm} install succeeded.")
                endif()
                break()
            endif()
        endforeach()

    elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
        foreach(_p
            "/opt/homebrew/opt/openssl@3"
            "/opt/homebrew/opt/openssl"
            "/usr/local/opt/openssl@3"
            "/usr/local/opt/openssl")
            if(EXISTS "${_p}/include/openssl/ssl.h")
                set(OPENSSL_ROOT_DIR "${_p}")
                message(STATUS "[OpenSSL] Homebrew at ${_p}")
                break()
            endif()
        endforeach()
        if(NOT OPENSSL_ROOT_DIR)
            find_program(_BREW brew)
            if(_BREW)
                execute_process(COMMAND "${_BREW}" install openssl@3
                    RESULT_VARIABLE _br OUTPUT_QUIET ERROR_QUIET)
                if(_br EQUAL 0)
                    execute_process(COMMAND "${_BREW}" --prefix openssl@3
                        OUTPUT_VARIABLE _bp OUTPUT_STRIP_TRAILING_WHITESPACE)
                    if(EXISTS "${_bp}/include/openssl/ssl.h")
                        set(OPENSSL_ROOT_DIR "${_bp}")
                    endif()
                endif()
            endif()
        endif()

    elseif(CMAKE_SYSTEM_NAME STREQUAL "Windows")
        foreach(_p
            "$ENV{OPENSSL_ROOT_DIR}"
            "C:/Program Files/OpenSSL-Win64"
            "C:/Program Files/OpenSSL"
            "C:/OpenSSL-Win64"
            "C:/OpenSSL"
            "$ENV{ProgramFiles}/OpenSSL-Win64")
            if(EXISTS "${_p}/include/openssl/ssl.h")
                set(OPENSSL_ROOT_DIR "${_p}")
                message(STATUS "[OpenSSL] Found at ${_p}")
                break()
            endif()
        endforeach()

        if(NOT OPENSSL_ROOT_DIR)
            find_program(_WINGET winget)
            if(_WINGET)
                message(STATUS "[OpenSSL] Trying winget install ShiningLight.OpenSSL.Dev")
                execute_process(
                    COMMAND "${_WINGET}" install -e --id ShiningLight.OpenSSL.Dev
                            --accept-source-agreements --accept-package-agreements
                            --silent
                    RESULT_VARIABLE _wg OUTPUT_QUIET ERROR_QUIET)
                if(_wg EQUAL 0)
                    foreach(_p "C:/Program Files/OpenSSL-Win64" "C:/Program Files/OpenSSL")
                        if(EXISTS "${_p}/include/openssl/ssl.h")
                            set(OPENSSL_ROOT_DIR "${_p}")
                            break()
                        endif()
                    endforeach()
                else()
                    message(STATUS "[OpenSSL] winget failed (rc=${_wg}), trying choco.")
                    find_program(_CHOCO choco)
                    if(_CHOCO)
                        execute_process(
                            COMMAND "${_CHOCO}" install openssl -y --no-progress
                            RESULT_VARIABLE _ch OUTPUT_QUIET ERROR_QUIET)
                        foreach(_p "C:/Program Files/OpenSSL-Win64" "C:/Program Files/OpenSSL")
                            if(EXISTS "${_p}/include/openssl/ssl.h")
                                set(OPENSSL_ROOT_DIR "${_p}")
                                break()
                            endif()
                        endforeach()
                    endif()
                endif()
            endif()
        endif()
    endif()
endif()

if(NOT FSTICKER_OPENSSL_FROM_SOURCE)
    if(WIN32)
        set(OPENSSL_USE_STATIC_LIBS TRUE)
    endif()
    find_package(OpenSSL QUIET)
endif()

if(OpenSSL_FOUND)
    message(STATUS "[OpenSSL] System: ${OPENSSL_VERSION}")
    message(STATUS "  inc: ${OPENSSL_INCLUDE_DIR}")
    message(STATUS "  ssl: ${OPENSSL_SSL_LIBRARY}")
    message(STATUS "  cry: ${OPENSSL_CRYPTO_LIBRARY}")
endif()

if(NOT OpenSSL_FOUND)
    message(STATUS "[OpenSSL] Not found — building from source via FetchContent.")
    include(FetchContent)
    FetchContent_Declare(openssl_cmake
        GIT_REPOSITORY https://github.com/janbar/openssl-cmake.git
        GIT_TAG        master
        GIT_SHALLOW    TRUE)
    set(BUILD_SHARED_LIBS    OFF CACHE BOOL "" FORCE)
    set(OPENSSL_BUILD_TESTS  OFF CACHE BOOL "" FORCE)
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
    FetchContent_MakeAvailable(openssl_cmake)

    if(NOT (TARGET ssl AND TARGET crypto))
        message(FATAL_ERROR "[OpenSSL] FetchContent build did not produce ssl/crypto targets.")
    endif()
    set(OPENSSL_INCLUDE_DIR
        "${openssl_cmake_SOURCE_DIR}/include"
        "${openssl_cmake_BINARY_DIR}/include")
    set(OPENSSL_SSL_LIBRARY    "$<TARGET_FILE:ssl>")
    set(OPENSSL_CRYPTO_LIBRARY "$<TARGET_FILE:crypto>")
    set(_OSSL_FC TRUE)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Windows" AND NOT _OSSL_FC AND OpenSSL_FOUND AND NOT OPENSSL_USE_STATIC_LIBS)
    get_filename_component(_ssl_dir "${OPENSSL_SSL_LIBRARY}" DIRECTORY)
    get_filename_component(_ssl_bin_dir "${_ssl_dir}/../bin" ABSOLUTE)
    set(OPENSSL_BIN_DIR "${_ssl_bin_dir}")
    file(GLOB _dlls
        "${_ssl_dir}/../bin/libssl*.dll"
        "${_ssl_dir}/../bin/libcrypto*.dll")
    foreach(_dll ${_dlls})
        configure_file("${_dll}"
            "${CMAKE_CURRENT_BINARY_DIR}/Release/" COPYONLY)
    endforeach()
endif()

if(OPENSSL_USE_STATIC_LIBS)
    set(_OSSL_LINK_STATIC 1)
else()
    set(_OSSL_LINK_STATIC 0)
endif()


if(_OSSL_FC)
    file(GENERATE OUTPUT "${OPENSSL_PATHS_FILE}" CONTENT
"OPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}
OPENSSL_SSL_LIBRARY=$<TARGET_FILE:ssl>
OPENSSL_CRYPTO_LIBRARY=$<TARGET_FILE:crypto>
OPENSSL_BIN_DIR=${OPENSSL_BIN_DIR}
OPENSSL_LINK_STATIC=${_OSSL_LINK_STATIC}
OPENSSL_FROM_FETCHCONTENT=1
")
else()
    file(WRITE "${OPENSSL_PATHS_FILE}"
"OPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}
OPENSSL_SSL_LIBRARY=${OPENSSL_SSL_LIBRARY}
OPENSSL_CRYPTO_LIBRARY=${OPENSSL_CRYPTO_LIBRARY}
OPENSSL_BIN_DIR=${OPENSSL_BIN_DIR}
OPENSSL_LINK_STATIC=${_OSSL_LINK_STATIC}
OPENSSL_FROM_FETCHCONTENT=0
")
endif()
message(STATUS "[OpenSSL] Paths written → ${OPENSSL_PATHS_FILE}")

option(FSTICKER_ENABLE_TIMESCALE "Build the optional TimescaleDB candle sink" ON)
option(FSTICKER_FORCE_BUNDLED_PQ "Always use the prebuilt static libpq, ignore system copy" ON)
set(FSTICKER_PQ_RELEASE_TAG "pq-REL_16_4" CACHE STRING
    "Release tag on Tapanhaz/fsticker-libpq-static to fetch prebuilt libpq from")
set(FSTICKER_PQ_REPO "Tapanhaz/fsticker-libpq-static" CACHE STRING
    "GitHub <owner>/<repo> hosting the prebuilt libpq release assets")

if(FSTICKER_ENABLE_TIMESCALE)
    set(_pq_ok FALSE)
    if(NOT FSTICKER_FORCE_BUNDLED_PQ)
        find_package(PostgreSQL QUIET)
        if(PostgreSQL_FOUND)
            message(STATUS "[libpq] Using system libpq ${PostgreSQL_VERSION_STRING} (NOT statically bundled -- dev build only)")
            set(FSTICKER_PQ_INCLUDE_DIRS ${PostgreSQL_INCLUDE_DIRS})
            set(FSTICKER_PQ_LIBRARIES    ${PostgreSQL_LIBRARIES})
            set(_pq_ok TRUE)
        endif()
    endif()

    if(NOT _pq_ok)
    
        if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
            if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$")
                set(_pq_plat "linux-x86_64")
            elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
                set(_pq_plat "linux-aarch64")
            endif()
        elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
            if(CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
                set(_pq_plat "macos-arm64")
            elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64)$")
                set(_pq_plat "macos-x86_64")
            endif()
        elseif(CMAKE_SYSTEM_NAME STREQUAL "Windows")
            if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(AMD64|x86_64|amd64)$")
                set(_pq_plat "windows-amd64")
            elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(ARM64|arm64)$")
                set(_pq_plat "windows-arm64")
            endif()
        endif()
        if(NOT _pq_plat)
            message(FATAL_ERROR
                "[libpq] Unrecognized platform (CMAKE_SYSTEM_NAME=${CMAKE_SYSTEM_NAME}, "
                "CMAKE_SYSTEM_PROCESSOR=${CMAKE_SYSTEM_PROCESSOR}) -- no matching prebuilt "
                "libpq asset. Set FSTICKER_ENABLE_TIMESCALE=OFF, or FSTICKER_FORCE_BUNDLED_PQ=OFF "
                "to use a system libpq instead.")
        endif()

        # Sadly we have to restrict below 15.4. Not Supported by my libpq.
        if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
            set(_pq_min_macos "15.4")
            if(CMAKE_OSX_DEPLOYMENT_TARGET AND CMAKE_OSX_DEPLOYMENT_TARGET VERSION_LESS _pq_min_macos)
                message(FATAL_ERROR
                    "[libpq] CMAKE_OSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET} is below "
                    "${_pq_min_macos}, the minimum the bundled static libpq was built for. "
                    "Raise CMAKE_OSX_DEPLOYMENT_TARGET (and MACOSX_DEPLOYMENT_TARGET in the wheel "
                    "build env) to ${_pq_min_macos}+, or set FSTICKER_ENABLE_TIMESCALE=OFF.")
            endif()
        endif()

        set(_pq_base_url
            "https://github.com/${FSTICKER_PQ_REPO}/releases/download/${FSTICKER_PQ_RELEASE_TAG}")
        set(_pq_archive_name "libpq-${FSTICKER_PQ_RELEASE_TAG}-${_pq_plat}.tar.gz")
        
        string(REGEX REPLACE "^pq-" "" _pq_tag_suffix "${FSTICKER_PQ_RELEASE_TAG}")
        set(_pq_archive_name "libpq-${_pq_tag_suffix}-${_pq_plat}.tar.gz")

        set(_pq_sha_url    "${_pq_base_url}/${_pq_archive_name}.sha256")
        set(_pq_sha_file   "${CMAKE_CURRENT_BINARY_DIR}/${_pq_archive_name}.sha256")
        set(_pq_archive_url "${_pq_base_url}/${_pq_archive_name}")
        set(_pq_archive     "${CMAKE_CURRENT_BINARY_DIR}/${_pq_archive_name}")

        message(STATUS "[libpq] Fetching checksum: ${_pq_sha_url}")
        file(DOWNLOAD "${_pq_sha_url}" "${_pq_sha_file}" STATUS _pq_sha_status)
        list(GET _pq_sha_status 0 _pq_sha_rc)
        if(NOT _pq_sha_rc EQUAL 0)
            message(FATAL_ERROR "[libpq] Failed to fetch checksum file: ${_pq_sha_url}")
        endif()
        
        # "<hash>  <filename>"
        file(READ "${_pq_sha_file}" _pq_sha_contents)
        string(REGEX MATCH "^[0-9a-fA-F]+" _pq_expected_sha256 "${_pq_sha_contents}")
        if(NOT _pq_expected_sha256)
            message(FATAL_ERROR "[libpq] Could not parse checksum from ${_pq_sha_file}")
        endif()

        message(STATUS "[libpq] Fetching prebuilt static libpq: ${_pq_archive_url}")
        file(DOWNLOAD "${_pq_archive_url}" "${_pq_archive}"
             STATUS _pq_dl_status EXPECTED_HASH SHA256=${_pq_expected_sha256})
        list(GET _pq_dl_status 0 _pq_dl_rc)
        if(NOT _pq_dl_rc EQUAL 0)
            list(GET _pq_dl_status 1 _pq_dl_msg)
            message(FATAL_ERROR "[libpq] Download/verification failed for ${_pq_archive_url}: ${_pq_dl_msg}")
        endif()

        set(_pq_extract_dir "${CMAKE_CURRENT_BINARY_DIR}/libpq_prebuilt")
        file(REMOVE_RECURSE "${_pq_extract_dir}")
        file(MAKE_DIRECTORY "${_pq_extract_dir}")
        file(ARCHIVE_EXTRACT INPUT "${_pq_archive}" DESTINATION "${_pq_extract_dir}")

        set(FSTICKER_PQ_INCLUDE_DIRS "${_pq_extract_dir}/include")
        
        set(_pq_lib_order
            libpq
            libpgcommon
            libpgcommon_ryu
            libpgcommon_config_info
            libpgport)
        set(FSTICKER_PQ_LIBRARIES "")
        foreach(_pq_libname ${_pq_lib_order})
            file(GLOB _pq_lib_found
                "${_pq_extract_dir}/lib/${_pq_libname}.a"
                "${_pq_extract_dir}/lib/${_pq_libname}.lib")
            if(NOT _pq_lib_found)
                message(FATAL_ERROR
                    "[libpq] Prebuilt archive for ${_pq_plat} missing ${_pq_libname} "
                    "-- expected under ${_pq_extract_dir}/lib/")
            endif()
            list(APPEND FSTICKER_PQ_LIBRARIES ${_pq_lib_found})
        endforeach()
        message(STATUS "[libpq] Verified + extracted ${_pq_plat}: ${FSTICKER_PQ_LIBRARIES}")
    endif()

    file(APPEND "${OPENSSL_PATHS_FILE}"
"FSTICKER_PQ_INCLUDE_DIRS=${FSTICKER_PQ_INCLUDE_DIRS}
FSTICKER_PQ_LIBRARIES=${FSTICKER_PQ_LIBRARIES}
")
endif()
 
