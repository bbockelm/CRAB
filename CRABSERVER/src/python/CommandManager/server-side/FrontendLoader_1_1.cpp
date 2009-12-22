#include <boost/python.hpp>

#ifndef RUN_SER_GUARD
#define RUN_SER_GUARD
extern "C" { 
   #include "server2.h"
}
#endif

BOOST_PYTHON_MODULE(FrontendLoader_1_1)
{
    using namespace boost::python;
    def("start", run_service);
}

