#include "boost/python.hpp"
#include "CRAB_Proxy_API.h"

using namespace boost::python;

BOOST_PYTHON_MODULE(CRAB_Server_API_36X)
{
    class_<CRAB_Server_Session>("CRAB_Server_Session", init<std::string,int>())
        .def("transferTaskAndSubmit", &CRAB_Server_Session::transferTaskAndSubmit)
        .def("sendCommand", &CRAB_Server_Session::sendCommand)
        .def("getTaskStatus", &CRAB_Server_Session::getTaskStatus) 
    ;
}

