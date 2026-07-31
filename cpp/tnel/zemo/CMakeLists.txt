cmake_minimum_required(VERSION 3.16)



add_executable(metricproducer
        metricshm/metricproducer.cpp
)

target_link_libraries(metricproducer
        PRIVATE
        core
)




add_executable(metricreader
        metricshm/metricreader.cpp
)

target_link_libraries(metricreader
        PRIVATE
        core
)
