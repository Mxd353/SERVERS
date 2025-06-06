#pragma once

#include <Python.h>

#include <iostream>
#include <string>

inline std::string get_mac_from_python(const std::string &ip) {
  static bool python_initialized = false;
  if (!python_initialized) {
    Py_Initialize();
    PyRun_SimpleString("import sys");
    PyRun_SimpleString("sys.path.insert(0, './build/lib/arp')");
    python_initialized = true;
  }

  PyObject *pModule = PyImport_ImportModule("arp_module");
  if (!pModule) {
    PyErr_Print();
    std::cerr << "Failed to import arp_module" << std::endl;
    return "";
  }

  PyObject *pFunc = PyObject_GetAttrString(pModule, "get_mac");
  if (!pFunc || !PyCallable_Check(pFunc)) {
    PyErr_Print();
    std::cerr << "Function get_mac not found or not callable" << std::endl;
    Py_DECREF(pModule);
    return "";
  }

  PyObject *pArgs = PyTuple_Pack(1, PyUnicode_FromString(ip.c_str()));
  PyObject *pResult = PyObject_CallObject(pFunc, pArgs);
  Py_DECREF(pArgs);

  std::string result;
  if (pResult && PyUnicode_Check(pResult)) {
    result = PyUnicode_AsUTF8(pResult);
    Py_DECREF(pResult);
  } else {
    PyErr_Print();
    std::cerr << "Call to get_mac failed" << std::endl;
  }

  Py_DECREF(pFunc);
  Py_DECREF(pModule);

  return result;
}
