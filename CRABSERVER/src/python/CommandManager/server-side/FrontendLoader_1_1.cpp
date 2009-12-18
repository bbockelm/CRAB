#include <boost/python.hpp>

#ifndef RUN_SER_GUARD
#define RUN_SER_GUARD
int run_service(int port, char* logFile);
#endif

BOOST_PYTHON_MODULE(FrontendLoader_1_1)
{
    using namespace boost::python;
    def("start", run_service);
}

