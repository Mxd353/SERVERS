# lib/arp/CMakeLists.txt

find_package(Python3 REQUIRED COMPONENTS Interpreter)

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/arp.arp_module.so
    COMMAND ${Python3_EXECUTABLE} -m nuitka
            --module
            --output-dir=${CMAKE_CURRENT_BINARY_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/arp_module.py
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/arp_module.py
    COMMENT "Compiling arp_module.py to shared library with Nuitka"
)

add_custom_target(arp_py ALL
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/arp.arp_module.so
)
