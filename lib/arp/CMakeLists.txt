# lib/arp/CMakeLists.txt

find_package(Python3 REQUIRED COMPONENTS Interpreter)

# Nuitka compiled output with version info
set(ARP_NUITKA_SO "${CMAKE_CURRENT_BINARY_DIR}/arp.arp_module.so")
# Standard module name for Python import
set(ARP_MODULE_SO "${CMAKE_CURRENT_BINARY_DIR}/arp_module.so")

add_custom_command(
    OUTPUT ${ARP_NUITKA_SO}
    COMMAND ${Python3_EXECUTABLE} -m nuitka
            --module
            --output-dir=${CMAKE_CURRENT_BINARY_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/arp_module.py
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/arp_module.py
    COMMENT "Compiling arp_module.py to shared library with Nuitka"
)

# Create symlink for consistent module name
add_custom_command(
    OUTPUT ${ARP_MODULE_SO}
    COMMAND ${CMAKE_COMMAND} -E create_symlink
            ${ARP_NUITKA_SO}
            ${ARP_MODULE_SO}
    DEPENDS ${ARP_NUITKA_SO}
    COMMENT "Creating symlink for arp_module.so"
)

add_custom_target(arp_py ALL
    DEPENDS ${ARP_MODULE_SO}
)

# Export ARP module path for main project
set(ARP_MODULE_PATH ${CMAKE_CURRENT_BINARY_DIR} PARENT_SCOPE)
