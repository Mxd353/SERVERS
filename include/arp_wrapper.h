#pragma once

#include <Python.h>

#include <iostream>
#include <string>

// ARP module path from CMake configuration
#ifndef ARP_MODULE_PATH
#define ARP_MODULE_PATH "./build/lib/arp"
#endif

// Simple wrapper for ARP MAC address resolution using Python module
// Note: This function should be called only once at program startup
inline auto get_mac_from_python(const std::string &ip) -> std::string {
  // Initialize Python only if not already initialized
  bool initialized_by_me = false;
  if (!Py_IsInitialized()) {
    Py_Initialize();
    initialized_by_me = true;
  }

  PyRun_SimpleString("import sys");

  // Use the CMake-defined absolute path instead of hardcoded relative path
  std::string path_cmd =
      std::string("sys.path.insert(0, '") + ARP_MODULE_PATH + "')";
  PyRun_SimpleString(path_cmd.c_str());

  PyObject *module = PyImport_ImportModule("arp_module");
  if (!module) {
    PyErr_Print();
    std::cerr << "Failed to import arp_module from: " << ARP_MODULE_PATH
              << std::endl;
    return "";
  }

  PyObject *func = PyObject_GetAttrString(module, "get_mac");
  if (!func || !PyCallable_Check(func)) {
    PyErr_Print();
    std::cerr << "Function get_mac not found or not callable" << std::endl;
    Py_DECREF(module);
    return "";
  }

  PyObject *pArgs = PyTuple_Pack(1, PyUnicode_FromString(ip.c_str()));
  if (!pArgs) {
    PyErr_Print();
    Py_DECREF(func);
    Py_DECREF(module);
    return "";
  }

  PyObject *pResult = PyObject_CallObject(func, pArgs);
  Py_DECREF(pArgs);

  std::string result;
  if (pResult && PyUnicode_Check(pResult)) {
    const char *utf8_result = PyUnicode_AsUTF8(pResult);
    if (utf8_result) {
      result = utf8_result;
    }
    Py_DECREF(pResult);
  } else {
    if (!pResult) {
      PyErr_Print();
    }
    std::cerr << "Call to get_mac failed" << std::endl;
  }

  Py_DECREF(func);
  Py_DECREF(module);

  // Only finalize Python if we initialized it
  if (initialized_by_me) {
    Py_Finalize();
  }

  return result;
}
