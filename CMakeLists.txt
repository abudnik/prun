cmake_minimum_required(VERSION 2.6)
INCLUDE (CheckIncludeFiles)

project( prun )

set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
find_package(Boost 1.45.0 COMPONENTS system thread program_options filesystem REQUIRED)
include_directories( ${Boost_INCLUDE_DIR} )

if (Release)
  set(CMAKE_CXX_FLAGS "-Wall -Wextra -Wno-unused-parameter -Wno-long-long -pedantic -pthread -g -O3")
else()
  set(CMAKE_CXX_FLAGS "-Wall -Wextra -Wno-unused-parameter -Wno-long-long -pedantic -pthread -g -O0")
  add_definitions( -D_DEBUG )
endif()

set(SRC_DIR src)
include_directories ( ${SRC_DIR} )


set(COMMON_DIR ${SRC_DIR}/common)

CHECK_INCLUDE_FILES ("sys/prctl.h" HAVE_SYS_PRCTL_H)
CHECK_INCLUDE_FILES ("execinfo.h" HAVE_EXEC_INFO_H)
CONFIGURE_FILE(${COMMON_DIR}/configure.h.in ${COMMON_DIR}/configure.h)

set(SRC_COMMON_CPP log.cpp daemon.cpp config.cpp protocol.cpp json_rpc.cpp uuid.cpp)
foreach(cpp ${SRC_COMMON_CPP})
  list(APPEND SRC_COMMON ${COMMON_DIR}/${cpp} )
endforeach(cpp)
add_library(common STATIC ${SRC_COMMON})


set(APP_NAME pworker)

set(WORKER_DIR ${SRC_DIR}/worker)

set(SRC_PWORKER_CPP common.cpp master_ping.cpp job_completion_ping.cpp job_completion_table.cpp worker.cpp)
foreach(cpp ${SRC_PWORKER_CPP})
  list(APPEND SRC_PWORKER ${WORKER_DIR}/${cpp} )
endforeach(cpp)

add_executable( ${APP_NAME} ${SRC_PWORKER} )
target_link_libraries( ${APP_NAME} common ${Boost_LIBRARIES} -lrt -rdynamic )

set(APP_NAME prexec)

set(SRC_PREXEC_CPP common.cpp exec.cpp)
foreach(cpp ${SRC_PREXEC_CPP})
  list(APPEND SRC_PREXEC ${WORKER_DIR}/${cpp} )
endforeach(cpp)

add_executable( ${APP_NAME} ${SRC_PREXEC} )
target_link_libraries( ${APP_NAME} common ${Boost_LIBRARIES} -lrt -rdynamic )


set(APP_NAME pmaster)

set(MASTER_DIR ${SRC_DIR}/master)

set(SRC_MASTER_CPP defines.cpp ping.cpp node_ping.cpp job.cpp worker.cpp job_manager.cpp worker_manager.cpp scheduler.cpp statistics.cpp job_sender.cpp result_getter.cpp command_sender.cpp timeout_manager.cpp admin.cpp user_command.cpp master.cpp)
foreach(cpp ${SRC_MASTER_CPP})
  list(APPEND SRC_MASTER ${MASTER_DIR}/${cpp} )
endforeach(cpp)

add_executable( ${APP_NAME} ${SRC_MASTER} )
target_link_libraries( ${APP_NAME} common ${Boost_LIBRARIES} -lrt -rdynamic )


set(APP_NAME pmasterdb)

set(MASTERDB_DIR ${SRC_DIR}/masterdb)

set(SRC_MASTERDB_CPP defines.cpp session.cpp dbmemory.cpp masterdb.cpp)
foreach(cpp ${SRC_MASTERDB_CPP})
  list(APPEND SRC_MASTERDB ${MASTERDB_DIR}/${cpp} )
endforeach(cpp)

add_executable( ${APP_NAME} ${SRC_MASTERDB} )
target_link_libraries( ${APP_NAME} common ${Boost_LIBRARIES} -lrt -rdynamic )


if (Test)
  set(TEST_DIR ${SRC_DIR}/test)


  set(APP_NAME ptest_unit)

  set(SRC_TEST_UNIT_CPP unit.cpp)
  foreach(cpp ${SRC_TEST_UNIT_CPP})
    list(APPEND SRC_TEST_UNIT ${TEST_DIR}/${cpp} )
  endforeach(cpp)

  set(SRC_TEST_MASTER_CPP job.cpp worker.cpp job_manager.cpp worker_manager.cpp scheduler.cpp timeout_manager.cpp)
  foreach(cpp ${SRC_TEST_MASTER_CPP})
    list(APPEND SRC_TEST_UNIT ${MASTER_DIR}/${cpp} )
  endforeach(cpp)

  add_executable( ${APP_NAME} ${SRC_TEST_UNIT} )
  target_link_libraries( ${APP_NAME} common ${Boost_LIBRARIES} -lboost_unit_test_framework -lrt -rdynamic )


  set(APP_NAME ptest_load)

  set(SRC_TEST_LOAD_CPP load.cpp)
  foreach(cpp ${SRC_TEST_LOAD_CPP})
    list(APPEND SRC_TEST_LOAD ${TEST_DIR}/${cpp} )
  endforeach(cpp)

  set(SRC_TEST_MASTER_CPP job.cpp worker.cpp job_manager.cpp worker_manager.cpp scheduler.cpp timeout_manager.cpp)
  foreach(cpp ${SRC_TEST_MASTER_CPP})
    list(APPEND SRC_TEST_LOAD ${MASTER_DIR}/${cpp} )
  endforeach(cpp)

  add_executable( ${APP_NAME} ${SRC_TEST_LOAD} )
  target_link_libraries( ${APP_NAME} common ${Boost_LIBRARIES} -lboost_unit_test_framework -lrt -rdynamic )
endif()
