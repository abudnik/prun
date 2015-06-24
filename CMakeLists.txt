cmake_minimum_required(VERSION 2.6)
INCLUDE (CheckIncludeFiles)

project( prun )

set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
find_package(Boost 1.46.0 COMPONENTS system thread program_options filesystem REQUIRED)
include_directories( ${Boost_INCLUDE_DIR} )

if (Release)
  set(CMAKE_CXX_FLAGS "-std=c++0x -Wall -Wextra -Wno-unused-parameter -Wno-long-long -pedantic -pthread -g -O3")
else()
  set(CMAKE_CXX_FLAGS "-std=c++0x -Wall -Wextra -Wno-unused-parameter -Wno-long-long -pedantic -pthread -g -O0")
  add_definitions( -D_DEBUG )
endif()

set(SRC_DIR src)
include_directories ( ${SRC_DIR} )


set(COMMON_DIR ${SRC_DIR}/common)

CHECK_INCLUDE_FILES ("sys/prctl.h" HAVE_SYS_PRCTL_H)
CHECK_INCLUDE_FILES ("execinfo.h" HAVE_EXEC_INFO_H)

set(SRC_COMMON_CPP log.cpp daemon.cpp config.cpp protocol.cpp json_rpc.cpp uuid.cpp cron.cpp shared_library.cpp)
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

install(TARGETS
        ${APP_NAME}
    RUNTIME DESTINATION bin COMPONENT runtime)

set(APP_NAME prexec)

set(SRC_PREXEC_CPP common.cpp exec.cpp)
foreach(cpp ${SRC_PREXEC_CPP})
  list(APPEND SRC_PREXEC ${WORKER_DIR}/${cpp} )
endforeach(cpp)

add_executable( ${APP_NAME} ${SRC_PREXEC} )
target_link_libraries( ${APP_NAME} common ${Boost_LIBRARIES} -lrt -rdynamic )

install(FILES conf/worker.cfg DESTINATION /etc/pworker)
install(FILES node/node.java node/node.js node/node.py node/node.rb node/node.sh DESTINATION /var/lib/pworker/node)
install(FILES debian/prun-worker.init DESTINATION /etc/init.d) # used in rpm spec only
install(TARGETS
        ${APP_NAME}
    RUNTIME DESTINATION bin COMPONENT runtime)


set(APP_NAME pmaster)

set(MASTER_DIR ${SRC_DIR}/master)

set(SRC_MASTER_CPP defines.cpp ping.cpp node_ping.cpp job.cpp worker.cpp job_manager.cpp job_history.cpp worker_manager.cpp scheduler.cpp statistics.cpp job_sender.cpp result_getter.cpp command_sender.cpp timeout_manager.cpp cron_manager.cpp admin.cpp user_command.cpp master.cpp)
foreach(cpp ${SRC_MASTER_CPP})
  list(APPEND SRC_MASTER ${MASTER_DIR}/${cpp} )
endforeach(cpp)

add_executable( ${APP_NAME} ${SRC_MASTER} )
target_link_libraries( ${APP_NAME} common ${Boost_LIBRARIES} -lrt -rdynamic -ldl )

install(FILES conf/master.cfg conf/groups conf/hosts_group1 DESTINATION /etc/pmaster)
install(FILES jobs/cron.job jobs/test.job DESTINATION /var/lib/pmaster)
install(FILES jobs/example/date.sh jobs/example/hello.py DESTINATION /var/lib/pmaster/example)
install(FILES debian/prun-master.init DESTINATION /etc/init.d) # used in rpm spec only
install(TARGETS
        ${APP_NAME}
    RUNTIME DESTINATION bin COMPONENT runtime)


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


INSTALL(FILES prun DESTINATION bin)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/${COMMON_DIR}/configure.h.in ${CMAKE_CURRENT_SOURCE_DIR}/${COMMON_DIR}/configure.h)
