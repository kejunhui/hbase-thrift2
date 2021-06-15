CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(statistics)
SET (CMAKE_INCLUDE_CURRENT_DIR ON)
SET (CMAKE_CXX_FLAGS "-std=c++14 -O3")	
#add_definitions(-std=c++11) 
include_directories (
	./include/
	./include/hbase/
	./src/app
	./src/utils
	./src/tools
	/usr/include/boost	
	)

FILE (GLOB CASTER_SRCS
	./src/*.cpp
	./src/app/*.cpp
	./src/app/*.cc
	./src/utils/*.cpp
	./src/tools/*.cpp 
	./include/hbase/*.cpp/
	)
		
LINK_DIRECTORIES (/usr/local/lib /usr/lib)  		
		 
ADD_EXECUTABLE (statistics ${CASTER_SRCS})
FIND_PACKAGE (Boost COMPONENTS system thread REQUIRED)

TARGET_LINK_LIBRARIES (statistics ${Boost_LIBRARIES} libssl.so libcrypto.so  libthrift.so -ldl)
