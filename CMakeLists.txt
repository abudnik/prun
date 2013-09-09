cmake_minimum_required(VERSION 2.8)

project( prun )

set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
find_package(Boost COMPONENTS system thread program_options filesystem REQUIRED)

if (Release)
  set(CMAKE_CXX_FLAGS "-Wall -Wextra -Wno-unused-parameter -pedantic -pthread -g -O3")
else()
  set(CMAKE_CXX_FLAGS "-Wall -Wextra -Wno-unused-parameter -pedantic -pthread -g -O0")
endif()

set(SRC_DIR src)

set(COMMON_DIR ${SRC_DIR}/common)
set(WORKER_DIR ${SRC_DIR}/worker)

include_directories ( ${SRC_DIR} )

set(SRC_COMMON_WORKER_CPP log.cpp daemon.cpp config.cpp protocol.cpp)
foreach(cpp ${SRC_COMMON_WORKER_CPP})
  list(APPEND SRC_COMMON_WORKER ${COMMON_DIR}/${cpp} )
endforeach(cpp)

set(APP_NAME pyserver)

set(SRC_PYSERVER_CPP common.cpp master_ping.cpp job_completion_ping.cpp job_completion_table.cpp pyserver.cpp)
foreach(cpp ${SRC_PYSERVER_CPP})
  list(APPEND SRC_PYSERVER ${WORKER_DIR}/${cpp} )
endforeach(cpp)

set(SRC ${SRC_COMMON_WORKER} ${SRC_PYSERVER})

add_executable( ${APP_NAME} ${SRC} )
target_link_libraries( ${APP_NAME} ${Boost_LIBRARIES} -lrt )

set(APP_NAME pyexec)

set(SRC_PYEXEC_CPP common.cpp pyexec.cpp)
foreach(cpp ${SRC_PYEXEC_CPP})
  list(APPEND SRC_PYEXEC ${WORKER_DIR}/${cpp} )
endforeach(cpp)

set(SRC ${SRC_COMMON_WORKER} ${SRC_PYEXEC})

add_executable( ${APP_NAME} ${SRC} )
target_link_libraries( ${APP_NAME} ${Boost_LIBRARIES} -lrt )


set(MASTER_DIR ${SRC_DIR}/master)

set(APP_NAME master)

set(SRC_COMMON_MASTER_CPP log.cpp daemon.cpp config.cpp protocol.cpp)
foreach(cpp ${SRC_COMMON_MASTER_CPP})
  list(APPEND SRC_COMMON_MASTER ${COMMON_DIR}/${cpp} )
endforeach(cpp)

set(SRC_MASTER_CPP defines.cpp ping.cpp node_ping.cpp job.cpp worker.cpp job_manager.cpp worker_manager.cpp scheduler.cpp job_sender.cpp result_getter.cpp timeout_manager.cpp admin.cpp master.cpp)
foreach(cpp ${SRC_MASTER_CPP})
  list(APPEND SRC_MASTER ${MASTER_DIR}/${cpp} )
endforeach(cpp)

set(SRC ${SRC_COMMON_MASTER} ${SRC_MASTER})

add_executable( ${APP_NAME} ${SRC} )
target_link_libraries( ${APP_NAME} ${Boost_LIBRARIES} -lrt )
