cmake_minimum_required(VERSION 3.15)

project(DistrictSimMPI CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (MSVC)
    add_compile_options(/W4)
else ()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif ()
find_package(MPI REQUIRED)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

file(GLOB SRC_FILES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
)
add_executable(sim_mpi ${SRC_FILES})
target_link_libraries(sim_mpi PRIVATE MPI::MPI_CXX)
set_target_properties(sim_mpi PROPERTIES POSITION_INDEPENDENT_CODE ON)
